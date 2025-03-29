import sys
import logging
import datetime
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
import pprint # For pretty printing logs
import os # For environment variables

# --- Configuration ---
DEFAULT_COST_CATEGORY_VALUE = "Uncategorized"
ROOT_CATEGORY_NAME = "Root"
CATEGORY_NAME_SEPARATOR = "-"
TARGET_CE_REGION = 'us-east-1'

# --- Logger Setup ---
logger = logging.getLogger(__name__)
log_level_name_from_env = os.environ.get('LOG_LEVEL', 'INFO').upper()
numeric_log_level = getattr(logging, log_level_name_from_env, None)
if not isinstance(numeric_log_level, int):
    # Use root logger for warning as handler might not be set yet or level too high
    logging.warning(f"Invalid LOG_LEVEL: '{log_level_name_from_env}'. Defaulting to INFO.")
    numeric_log_level = logging.INFO
logger.setLevel(numeric_log_level)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(numeric_log_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.info(f"Logger initialized with level: {logging.getLevelName(logger.getEffectiveLevel())}")

# --- Boto3 Clients (Global Initialization) ---
org_client = None
ce_client = None
try:
    logger.debug("Initializing boto3 clients globally...")
    org_client = boto3.client('organizations')
    logger.info(f"Initializing Cost Explorer client in region: {TARGET_CE_REGION}")
    ce_client = boto3.client('ce', region_name=TARGET_CE_REGION)
    logger.debug("Boto3 clients initialized.")
except (NoCredentialsError, PartialCredentialsError) as e:
    logger.error(f"AWS credentials not found or incomplete: {e}")
    sys.exit(1)
except Exception as e:
    logger.exception(f"Failed to create boto3 clients: {e}")
    sys.exit(1)

# --- Helper Function for Pagination ---
def get_paginated_results(client, operation_name, result_key, **kwargs):
    """ Helper function for boto3 list operations using paginators. Propagates exceptions. """
    if not client:
        logger.error(f"Boto3 client for operation '{operation_name}' not initialized.")
        raise ValueError(f"Boto3 client for {operation_name} is not available.")
    logger.debug(f"Paginating operation '{operation_name}' for key '{result_key}' with args: {kwargs}")
    try:
        paginator = client.get_paginator(operation_name)
    except Exception as paginator_err:
         logger.error(f"Could not get paginator for {operation_name}. Error: {paginator_err}")
         raise paginator_err
    results = []
    page_iterator = paginator.paginate(**kwargs)
    for page in page_iterator:
        results.extend(page.get(result_key, []))
    logger.debug(f"Pagination complete for '{operation_name}', found {len(results)} items.")
    return results

# --- Helper for OU Name Resolution ---
ou_name_cache = {} # Cache {ou_id: ou_name}
def get_ou_name(ou_id):
    """ Gets OU name from ID using cache or describe_organizational_unit API. Propagates exceptions. """
    if ou_id in ou_name_cache:
        return ou_name_cache[ou_id]
    # Propagate exceptions from API call
    logger.debug(f"Calling describe_organizational_unit for ID: {ou_id}")
    response = org_client.describe_organizational_unit(OrganizationalUnitId=ou_id)
    name = response['OrganizationalUnit']['Name']
    ou_name_cache[ou_id] = name
    return name

# --- Core Functions ---
def get_organization_structure(max_depth):
    """ Retrieves Org structure assigning accounts to deepest category up to max_depth. Returns dict or None. Propagates exceptions. """
    logger.info(f"Fetching organization structure (assigning accounts to deepest path up to depth {max_depth})...")
    structure = {}
    ou_name_cache.clear()
    # Propagate exceptions from API calls
    logger.info("Listing all accounts in the organization...")
    all_accounts = get_paginated_results(org_client, 'list_accounts', 'Accounts')
    logger.info(f"Found {len(all_accounts)} accounts.")
    if not all_accounts:
        logger.warning("No accounts found in the organization.")
        return {ROOT_CATEGORY_NAME: []}

    account_count = 0
    total_accounts = len(all_accounts)
    logger.info(f"Processing path for {total_accounts} accounts...")
    for account in all_accounts:
        account_count += 1; account_id = account['Id']
        if account_count % 100 == 0 or account_count == total_accounts: logger.info(f"Processing account {account_count}/{total_accounts}: ID {account_id}")
        else: logger.debug(f"Processing account {account_count}/{total_accounts}: ID {account_id}")

        path_components = []
        current_child_id = account_id
        while True:
            logger.debug(f"Listing parents for ChildId: {current_child_id}")
            parents_response = org_client.list_parents(ChildId=current_child_id) # Propagates exceptions
            parents = parents_response.get('Parents', [])
            if not parents:
                if not path_components: logger.debug(f"Account {account_id} appears to be directly under Root.")
                else: logger.error(f"Path to root broken for account {account_id} (stopped at {current_child_id}). Assigning to Root.")
                path_components = [] # Assign to Root if path is broken or no OU parent
                break
            parent = parents[0]
            parent_id, parent_type = parent['Id'], parent['Type']
            if parent_type == 'ORGANIZATIONAL_UNIT':
                ou_name = get_ou_name(parent_id) # Propagates exceptions
                path_components.append(ou_name)
                current_child_id = parent_id
            elif parent_type == 'ROOT': break
            else: logger.warning(f"Unexpected parent type '{parent_type}'. Stopping path traversal."); break
        path_components.reverse()

        category_name = ROOT_CATEGORY_NAME
        effective_depth = len(path_components)
        if effective_depth > 0:
            category_name = CATEGORY_NAME_SEPARATOR.join(path_components[:max_depth])
        logger.debug(f"Account {account_id} assigned to category: '{category_name}' (Path: {' -> '.join(path_components)}, Effective Depth: {effective_depth})")
        if category_name not in structure: structure[category_name] = []
        structure[category_name].append(account_id)

    logger.info("Successfully processed all accounts and determined categories.")
    if ROOT_CATEGORY_NAME not in structure: structure[ROOT_CATEGORY_NAME] = []
    logger.info(f"Final structure has {len(structure)} categories: {list(structure.keys())}")
    logger.debug(f"Final structure details (may be large): {structure}")
    return structure

def build_cost_category_rules(org_structure):
    """ Builds rule list. Skips categories with empty accounts. Propagates exceptions. """
    logger.info("Building Cost Category rules...")
    rules = []
    if not org_structure: return rules
    for category_name in sorted(org_structure.keys()):
        account_ids = org_structure.get(category_name)
        if not account_ids or not isinstance(account_ids, list):
             logger.warning(f"Skipping rule for '{category_name}' due to missing/invalid/empty account list.")
             continue
        if len(account_ids) > 1000: logger.warning(f"Category '{category_name}' has {len(account_ids)} accounts (> 1000 limit).")
        rules.append({
            'Value': category_name,
            'Rule': {'Dimensions': {'Key': 'LINKED_ACCOUNT', 'Values': account_ids, 'MatchOptions': ['EQUALS']}},
        })
    if len(rules) > 500: logger.warning(f"Generated {len(rules)} rules (> 500 limit).")
    logger.info(f"Built {len(rules)} rules for categories with accounts.")
    return rules

def find_cost_category_arn(cost_category_name):
    """ Finds ARN using manual pagination. Returns ARN or None. Propagates exceptions. """
    logger.info(f"Checking if Cost Category '{cost_category_name}' exists (manual pagination)...")
    next_token = None
    page_count, definition_count = 0, 0
    while True:
        page_count += 1; kwargs = {}
        if next_token: kwargs['NextToken'] = next_token
        logger.debug(f"Calling list_cost_category_definitions (Page {page_count}) with args: {kwargs}")
        response = ce_client.list_cost_category_definitions(**kwargs) # Propagates exceptions
        definitions = response.get('CostCategoryReferences', [])
        logger.debug(f"Page {page_count}: Received {len(definitions)} definitions.")
        for definition in definitions:
            definition_count += 1
            retrieved_name, retrieved_arn = definition.get('Name'), definition.get('CostCategoryArn')
            logger.debug(f"Comparing: Argument='{cost_category_name}' vs Retrieved='{retrieved_name}' (CostCategoryArn: {retrieved_arn})")
            if retrieved_name == cost_category_name and retrieved_arn:
                logger.info(f"Found matching Cost Category '{cost_category_name}' with ARN: {retrieved_arn}")
                return retrieved_arn
        next_token = response.get('NextToken')
        if not next_token: break
    logger.info(f"Cost Category '{cost_category_name}' not found after checking {definition_count} definitions across {page_count} pages.")
    return None

def put_cost_category(cost_category_name, rules, default_value, effective_start_iso_str):
    """ Creates/Updates Cost Category. Returns True on success. Raises exceptions on failure. Logs raw parameters. """
    cost_category_arn = find_cost_category_arn(cost_category_name)
    # If find_cost_category_arn raises an exception, it propagates up

    common_args_for_api = {
        'RuleVersion': 'CostCategoryExpression.v1',
        'Rules': rules,
        'DefaultValue': default_value,
        'EffectiveStart': effective_start_iso_str
    }

    # --- Pre-check API Limits ---
    if len(rules) > 500: error_msg = f"Num rules ({len(rules)}) > 500 limit."; logger.error(error_msg); raise ValueError(error_msg)
    for i, rule in enumerate(rules):
         try:
              num_accounts = len(rule['Rule']['Dimensions']['Values'])
              if num_accounts == 0: logger.warning(f"Rule {i+1} ('{rule.get('Value','N/A')}') has 0 accounts.")
              elif num_accounts > 1000: error_msg = f"Rule {i+1} ('{rule.get('Value','N/A')}') > 1000 account limit ({num_accounts})."; logger.error(error_msg); raise ValueError(error_msg)
         except (KeyError, TypeError): logger.warning(f"Rule {i+1} ('{rule.get('Value','N/A')}') structure invalid for limit check.")

    # --- Log and Prepare Final Parameters ---
    logger.info("Attempting to call Cost Explorer API...")
    params_to_pass = common_args_for_api.copy()
    if cost_category_arn:
        api_action = "update_cost_category_definition"
        params_to_pass['CostCategoryArn'] = cost_category_arn
    else:
        api_action = "create_cost_category_definition"
        params_to_pass['Name'] = cost_category_name

    pretty_log_params = pprint.pformat(params_to_pass, indent=2, width=120, sort_dicts=False)
    logger.info(f"API Action: {api_action}")
    logger.info(f"Parameters:\n{pretty_log_params}")

    # --- Perform API Call ---
    # Propagate exceptions from API call
    if api_action == "update_cost_category_definition":
        logger.info(f"Updating existing Cost Category: {cost_category_arn}")
        response = ce_client.update_cost_category_definition(**params_to_pass)
        logger.info(f"Successfully updated Cost Category. ARN: {response.get('CostCategoryArn')}, Effective Start: {response.get('EffectiveStart')}")
    elif api_action == "create_cost_category_definition":
        logger.info(f"Creating new Cost Category: {cost_category_name}")
        response = ce_client.create_cost_category_definition(**params_to_pass)
        logger.info(f"Successfully created Cost Category. ARN: {response.get('CostCategoryArn')}, Effective Start: {response.get('EffectiveStart')}")
    else: raise RuntimeError("Internal error determining API action.")
    return True

# --- Parameter Handling ---
def get_parameters():
    """ Parses args: Name, StartMonth (YYYY-MM), Depth (int >= 1). Returns tuple or exits. """
    logger.debug("Parsing command line arguments...")
    if len(sys.argv) != 4: logger.error("Usage: python3 put_ou_cost_category.py <Name> <YYYY-MM> <Depth>"); sys.exit(1)
    cost_category_name, effective_start_month, depth_str = sys.argv[1], sys.argv[2], sys.argv[3]
    try:
        start_month_date = datetime.datetime.strptime(effective_start_month, '%Y-%m').date()
        effective_start_datetime_utc = datetime.datetime.combine(start_month_date.replace(day=1), datetime.time(0, 0, 0, tzinfo=datetime.timezone.utc))
        effective_start_iso_str = effective_start_datetime_utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        current_time = datetime.datetime.now(datetime.timezone.utc)
        if start_month_date.replace(day=1) > current_time.date().replace(day=1): logger.warning(f"Effective start {start_month_date.replace(day=1).strftime('%Y-%m-%d')} is future.")
    except ValueError: logger.error(f"Invalid YYYY-MM format: '{effective_start_month}'."); sys.exit(1)
    try:
        depth = int(depth_str); assert depth >= 1
    except (ValueError, AssertionError): logger.error(f"Invalid Depth: '{depth_str}'. Must be integer >= 1."); sys.exit(1)
    logger.info(f"Target Name: {cost_category_name}, Start: {effective_start_iso_str}, Depth: {depth}")
    return cost_category_name, effective_start_iso_str, depth

# --- Main Execution Logic ---
def main(cost_category_name, effective_start_iso_str, depth):
    """ Main logic. Returns True on success, raises exceptions on failure. """
    logger.info("Main processing started.")
    org_data = get_organization_structure(depth)
    if org_data is None: raise RuntimeError("Org Root not found or initial fetch failed.")
    if not org_data or all(not v for v in org_data.values()): logger.warning("No accounts found for depth. Proceeding with empty rules."); cost_category_rules = []
    else: cost_category_rules = build_cost_category_rules(org_data)
    logger.info(f"Built {len(cost_category_rules)} rules.")
    put_cost_category(cost_category_name, cost_category_rules, DEFAULT_COST_CATEGORY_VALUE, effective_start_iso_str)
    logger.info("Main processing finished successfully.")
    return True

# --- Script Entry Point ---
if __name__ == "__main__":
    logger.info("Script execution started.")
    exit_code = 0
    try:
        name_arg, start_arg, depth_arg = get_parameters()
        logger.info(f"Parameters: name='{name_arg}', start='{start_arg}', depth={depth_arg}")
        main(name_arg, start_arg, depth_arg) # Propagates exceptions
        logger.info("Script finished successfully.")
    except (ClientError, ValueError, RuntimeError, Exception) as e:
        logger.exception(f"An error occurred during script execution: {e}")
        exit_code = 1
    finally:
        logger.info(f"Script execution finished with exit code {exit_code}.")
        sys.exit(exit_code)