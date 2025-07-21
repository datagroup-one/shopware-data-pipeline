# Shopware Lambda Validation Functions

## Overview

This documentation covers the design, setup, usage, and validation logic behind two AWS Lambda functions:

* `pos-validation-fn`
* `inventory-validation-fn`

These functions form the **validation layer** of the Shopware data pipeline. Each is triggered by an S3 event notification on specific folders within `shopware.bucket`. The goal is to validate raw incoming data before it flows into the raw layer (`raw-data`) or is rejected.

---

## Workflow Summary

| Bucket/Folders               | Lambda Triggered          | Valid Data Path                           | Invalid Data Path                |
| ---------------------------- | ------------------------- | ----------------------------------------- | -------------------------------- |
| `shopware.bucket/pos/`       | `pos-validation-fn`       | `raw-data/pos/yyyy/mm/dd/file.csv`        | `rejected/pos/yyyy/mm/dd/`       |
| `shopware.bucket/inventory/` | `inventory-validation-fn` | `raw-data/inventory/yyyy/mm/dd/file.json` | `rejected/inventory/yyyy/mm/dd/` |

Each function performs schema validation, formatting checks, and error tagging. Rejected files or records are preserved with rejection reasons for traceability.

---

## POS Validation Lambda (`pos-validation-fn`)

> This function is designed to validate CSV-formatted POS files.

### Validation Criteria (POS)

Each record must:

* Be properly parsed into columns
* Contain these **required fields**:

  * `transaction_id` (string)
  * `store_id` (int)
  * `product_id` (int)
  * `quantity` (int)
  * `revenue` (float)
  * `timestamp` (float)
* Optional fields like `discount_applied` are permitted

### Invalid Criteria (POS)

* Missing required fields
* Type mismatch (e.g. `revenue` is a string instead of a float)
* Ill-formatted lines in CSV

### Output Behavior

* Valid CSV lines → moved to `raw-data/pos/yyyy/mm/dd/`
* Invalid → written to `rejected/pos/yyyy/mm/dd/`


---

## Inventory Validation Lambda (`inventory-validation-fn`)

> Validates structured JSON arrays uploaded to the inventory folder.

### Validation Schema (Inventory)

**Required Fields:**

* `inventory_id` (int)
* `product_id` (int)
* `warehouse_id` (int)
* `stock_level` (int)
* `last_updated` (float)

**Optional Fields:**

* `restock_threshold` (int)

### Validation Logic

```python
for field, field_type in REQUIRED_FIELDS.items():
    if field not in record or record[field] is None:
        return False, f"Missing required field: {field}"
    try:
        field_type(record[field])
    except:
        return False, f"Invalid type for field: {field}"
```

### Fails if:

* File is not valid JSON
* JSON is not a list of dictionaries
* Missing fields or wrong types
* Optional field present but malformed (e.g., `restock_threshold = 'ten'`)

### File-Level Handling

* If the file fails JSON parsing → moved as-is to `rejected/inventory/yyyy/mm/dd/`
* If some records are valid and others aren’t:

  * Valid → written to `raw-data/inventory/yyyy/mm/dd/`
  * Invalid → written to `rejected/inventory/yyyy/mm/dd/`

### Output File Format

JSON files written to S3 are pretty-formatted and UTF-8 encoded.

---

## 🛠 Deployment & Setup

### Required IAM Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": "arn:aws:s3:::shopware.bucket/*"
    },
    {
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "*"
    }
  ]
}
```

### S3 Event Notification Setup

* Source: `shopware.bucket`
* Prefix: `pos/` or `inventory/`
* Event type: `s3:ObjectCreated:*`
* Destination: respective Lambda ARN


## User Guide

### How to Use

1. Drop a new file into `shopware.bucket/pos/` or `.../inventory/`
2. Lambda is auto-triggered
3. Results appear in either `raw-data/...` or `rejected/...`

### How to Validate a New File

1. Manually upload to S3 via console or CLI
2. Go to **CloudWatch Logs**
3. Check logs for:

   * Validation stats (valid/invalid count)
   * Any file-level rejections

### Monitoring & Troubleshooting

* Logs are automatically streamed to CloudWatch
* Invalid files/records are saved with human-readable rejection reasons
* Use S3 versioning to track overwrites if enabled

---

## Conclusion

These validation Lambdas form the first critical step in the Shopware data pipeline. They:

* Guarantee schema quality before ETL
* Provide traceability for rejections
* Enable scalable, automated batch data validation

Future extensions could include:

* Integration with Amazon EventBridge for alerts
* Tracking rejections in DynamoDB for audit purposes
* Parameterizing schemas via SSM for flexibility
