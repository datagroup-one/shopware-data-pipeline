# Shopware KPI Computation Lambda - Setup & User Manual

## Overview

This document serves as a comprehensive guide for the setup, configuration, and usage of the Shopware KPI Computation Lambda function. The solution is designed to support department-specific KPIs (Sales and Operations) using Amazon Redshift as the analytical backend. The function ensures scalable, secure, and maintainable generation of data-driven views.

---

## Objectives

* Automate the creation of Redshift KPI views.
* Partition views by department to enforce RBAC (Role-Based Access Control).
* Ensure retry logic and error handling for resiliency.
* Maintain a structured and secure analytics architecture.

---

## Features

| Feature                        | Description                                                                               |
| ------------------------------ | ----------------------------------------------------------------------------------------- |
| Departmental schema partition  | Sales and Ops KPIs are created in separate Redshift schemas: `sales_kpis` and `ops_kpis`. |
| Individual KPI logic preserved | KPI business logic remains unaltered from original specification.                         |
| Secure data access via RBAC    | Redshift groups and schema-level privileges used to restrict access.                      |
| View creation & retry logic    | Internal retry loop with logging; robust against transient failures.                      |
| Schema auto-creation in Lambda | The function automatically creates the schemas if they don't exist.                       |

---

## Setup Instructions

### Prerequisites

* AWS Lambda execution role with necessary permissions.
* Amazon Redshift Serverless or RA3 cluster with Data API enabled.
* Secrets Manager secret storing Redshift credentials.

### 1. Deploy the Lambda Function

1. Create a new Lambda function in Python 3.12.
2. Add the environment variables:

| Key                   | Example Value                       |
| --------------------- | ----------------------------------- |
| `DB_NAME`             | `shopwaredb`                        |
| `REDSHIFT_WORKGROUP`  | `shopware-redshift-wg`              |
| `REDSHIFT_SECRET_ARN` | `arn:aws:secretsmanager:...:secret` |

3. Paste in the Lambda code provided in the deployment script.

### 2. IAM Role Permissions

The Lambda execution role should include the following managed policies:

* `AmazonRedshiftDataFullAccess`
* `SecretsManagerReadWrite`

Or attach a custom policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "redshift-data:*"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "<your-secret-arn>"
    }
  ]
}
```

---

## User Guide

### Usage Workflow

1. Trigger the Lambda manually or as part of a Step Function or EventBridge rule.
2. The Lambda:

   * Ensures Redshift schemas `sales_kpis` and `ops_kpis` exist.
   * Creates or replaces KPI views per department.
   * Retries on failure and logs progress to CloudWatch.
3. Users in appropriate Redshift groups can query views:

   * Sales team uses: `sales_kpis.sales_by_region_product`, `sales_kpis.turnover_rate`, etc.
   * Operations team uses: `ops_kpis.inventory_turnover`, `ops_kpis.stockout_alerts`, etc.

### Access Pattern

| Department | Access Type      | Tool                           |
| ---------- | ---------------- | ------------------------------ |
| Sales      | Data Mart (Read) | QuickSight, Tableau, Metabase  |
| Operations | SQL Analytics    | Redshift Query Editor, DBeaver |

---

## Security & Governance

### RBAC Setup Example

```sql
-- Sales Analysts
CREATE GROUP sales_analysts;
GRANT USAGE ON SCHEMA sales_kpis TO GROUP sales_analysts;
GRANT SELECT ON ALL TABLES IN SCHEMA sales_kpis TO GROUP sales_analysts;
ALTER DEFAULT PRIVILEGES IN SCHEMA sales_kpis GRANT SELECT ON TABLES TO GROUP sales_analysts;

-- Operations Analysts
CREATE GROUP ops_analysts;
GRANT USAGE ON SCHEMA ops_kpis TO GROUP ops_analysts;
GRANT SELECT ON ALL TABLES IN SCHEMA ops_kpis TO GROUP ops_analysts;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops_kpis GRANT SELECT ON TABLES TO GROUP ops_analysts;
```

### Compliance Considerations

* **Data encryption**: Handled via Redshift + KMS (at rest) and SSL (in transit).
* **Audit logs**: Enable Redshift user activity logging for traceability.
* **Access scoping**: Only grant SELECT to approved teams.

---

## Manual Notes

* Lambda retries queries 3 times before failing.
* Any schema or SQL-level failure will log clearly in CloudWatch.
* You can extend view logic or create department-specific dashboards as needed.
* This pattern supports future schemas like `marketing_kpis`, `finance_kpis`, etc.

---

## Conclusion

This KPI pipeline provides a scalable, secure, and auditable system for delivering operational and sales insights to Shopware teams. By aligning with best practices around data modeling, RBAC, and modular design, this Lambda function supports maintainable enterprise-grade analytics.
