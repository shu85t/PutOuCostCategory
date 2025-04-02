# AWS OU Cost Category Updater

**This script is a sample implementation for the method described in the following article:**

**[AWS Cost Categories with OU Structure](https://dev.to/shu85t/aws-cost-categories-with-ou-structure-61n)**

## Overview

This Python script automatically creates or updates an AWS Cost Explorer Cost Category based on the structure of your AWS Organizations Organizational Units (OUs). It generates category names reflecting the OU hierarchy (e.g., `Root`, `OU1`, `OU1-OU1A`) up to a specified depth, and assigns each account to its deepest associated category within that depth limit.

This helps visualize and analyze costs according to your defined organizational structure.

**Disclaimer:** This script is provided as a sample. Please review the code and test thoroughly in your environment before applying it to production cost data. Pay close attention to the security considerations below.

## Features

* Fetches the complete list of accounts and their OU paths within the AWS Organization.
* Generates Cost Category rule values based on OU paths, separated by hyphens (`-`), up to a specified `depth`. (e.g., `Level1OU-Level2OU`)
* Assigns each AWS account uniquely to the category representing its deepest OU path within the specified `depth` limit. Accounts directly under Root are assigned to the `Root` category.
* Creates a new Cost Category if one with the specified name doesn't exist, or updates the existing one (idempotent `put` operation).
* Allows specifying the effective start month (`YYYY-MM`) for the Cost Category rules.
* Supports log level control via the `LOG_LEVEL` environment variable (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`).

## Prerequisites

* **Python:** Version 3.9 or later.
* **Boto3:** AWS SDK for Python (`pip install boto3`). Ensure you are using a recent version.
* **AWS Account:** Access to the **Management Account** of your AWS Organization.

## Execution Environment

* This script needs to run in an environment with credentials for the **Management Account** of your AWS Organization, possessing the necessary IAM permissions (see below).
* A primary intended environment is **AWS CloudShell** accessed while logged into the Management Account.
* It can also run on an EC2 instance or in an AWS Lambda function within the Management Account, provided the execution role has the required permissions.

## Usage

### Command Line Execution

```bash
python3 put_ou_cost_category.py <CostCategoryName> <EffectiveStartMonth> <Depth>
```

### Arguments

1.  **`<CostCategoryName>`** (Required): The name of the Cost Category to create or update (e.g., `OrganizationStructure`, `OUHierarchy`).
2.  **`<EffectiveStartMonth>`** (Required): The effective start month in `YYYY-MM` format (e.g., `2025-04`). The script uses the first day of this month (UTC midnight) for the API call.
3.  **`<Depth>`** (Required): An integer (1 or greater) specifying the maximum depth of the OU hierarchy to create categories for.
    * `1`: Creates categories for `Root` and first-level OUs.
    * `2`: Creates categories for `Root`, first-level OUs, and second-level OUs (e.g., `OU1-OU1A`).
    * Accounts are always assigned to their deepest category up to the specified depth.

### Log Level Configuration

Control logging verbosity using the `LOG_LEVEL` environment variable. Default is `INFO`.

```bash
# Example: Run with DEBUG logs
export LOG_LEVEL=DEBUG
python3 put_ou_cost_category.py MyOUCategory 2025-04 2

# Or temporarily for one command
LOG_LEVEL=DEBUG python3 put_ou_cost_category.py MyOUCategory 2025-04 2
```

## IAM Permissions

The IAM principal (user or role) running this script needs the following permissions:

**Required Actions:**

* `organizations:ListAccounts`
* `organizations:ListParents`
* `organizations:DescribeOrganizationalUnit`
* `organizations:ListRoots`
* `ce:ListCostCategoryDefinitions`
* `ce:CreateCostCategoryDefinition`
* `ce:UpdateCostCategoryDefinition`

**Example IAM Policy:**

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "OrganizationReadAccess",
            "Effect": "Allow",
            "Action": [
                "organizations:ListAccounts",
                "organizations:ListParents",
                "organizations:DescribeOrganizationalUnit",
                "organizations:ListRoots"
            ],
            "Resource": "*"
        },
        {
            "Sid": "CostCategoryReadWriteAccess",
            "Effect": "Allow",
            "Action": [
                "ce:ListCostCategoryDefinitions",
                "ce:CreateCostCategoryDefinition",
                "ce:UpdateCostCategoryDefinition"
            ],
            "Resource": "*"
        }
    ]
}
```

**Note on Permissions:** Using `Resource: "*"` grants broad permissions. While necessary for some `organizations:*` actions and listing cost categories, consider if you can scope down the `ce:CreateCostCategoryDefinition` and `ce:UpdateCostCategoryDefinition` actions to specific Cost Category ARNs or based on naming conventions if your security policy requires it (though dynamic creation makes this difficult).

## Security Considerations

**IMPORTANT:** This script is intended as a sample. The AWS Organizations Management Account holds powerful permissions over your entire organization.

* **Best Practice:** AWS recommends minimizing operations performed directly within the Management Account.
* **Recommendation:** For regular use, especially in production environments, it is **strongly recommended** to run this script using an IAM role with **least privilege**. Create a dedicated IAM role specifically for this script, attach a policy containing *only* the required actions listed above, and use this role for execution (e.g., as the Lambda execution role or attached to the EC2/CloudShell environment). Avoid running scripts with broad administrative privileges.

**Always review and understand the script and the permissions granted before execution in your Management Account.**

## Other Notes

* **API Calls & Performance:** For large organizations or deep `depth` values, the script may make numerous API calls, potentially leading to longer execution times or API throttling. Consider this for scheduling and Lambda timeout settings.
* **Cost Category Update Latency:** Changes to Cost Categories can take up to 24 hours to fully reflect in Cost Explorer.
* **Error Handling:** Basic top-level exception handling is implemented. Retries for throttling are not included.
