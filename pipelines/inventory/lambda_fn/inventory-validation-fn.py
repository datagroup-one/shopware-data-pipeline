import boto3
import json
import logging
import re
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Required & optional schema fields
REQUIRED_FIELDS = {
    "inventory_id": int,
    "product_id": int,
    "warehouse_id": int,
    "stock_level": int,
    "last_updated": float,
}

OPTIONAL_FIELDS = {
    "restock_threshold": int,
}

EXPECTED_FIELDS = list(REQUIRED_FIELDS.keys()) + list(OPTIONAL_FIELDS.keys())


def validate_record(record):
    for field, field_type in REQUIRED_FIELDS.items():
        if field not in record or record[field] is None:
            return False, f"Missing required field: {field}"
        try:
            field_type(record[field])
        except (ValueError, TypeError):
            return False, f"Invalid type for field: {field}"

    if "restock_threshold" in record and record["restock_threshold"] is not None:
        try:
            int(record["restock_threshold"])
        except (ValueError, TypeError):
            return False, "Invalid type for restock_threshold"

    return True, None


def lambda_handler(event, context):
    for record in event['Records']:
        try:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            filename = key.split("/")[-1]
            logger.info(f"[START] Processing file: s3://{bucket}/{key}")

            response = s3.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')

            try:
                data = json.loads(content)
                assert isinstance(data, list)
            except Exception as e:
                logger.error(f"[ERROR] Invalid JSON structure in {key}: {str(e)}")
                move_to_rejected(bucket, filename, content, "Invalid JSON format")
                continue

            valid, invalid = [], []
            for item in data:
                is_valid, reason = validate_record(item)
                if is_valid:
                    valid.append(item)
                else:
                    item["rejection_reason"] = reason
                    invalid.append(item)

            date_folder = extract_date_folder(filename)

            if valid:
                raw_key = f"raw-data/inventory/{date_folder}/{filename}"
                write_json_to_s3(valid, bucket, raw_key)
                logger.info(f"[VALID] {len(valid)} records saved to {raw_key}")

            if invalid:
                rejected_key = f"rejected/inventory/{date_folder}/{filename}"
                write_json_to_s3(invalid, bucket, rejected_key)
                logger.warning(f"[INVALID] {len(invalid)} records saved to {rejected_key}")

            logger.info(f"[DONE] Finished processing {key}")

        except Exception as e:
            logger.error(f"[EXCEPTION] Unexpected error for file {key}: {str(e)}", exc_info=True)


def extract_date_folder(filename):
    match = re.search(r'inventory_(\d{8})_', filename)
    if match:
        date_str = match.group(1)
        return f"{date_str[:4]}/{date_str[4:6]}/{date_str[6:]}"
    logger.warning("[WARNING] Could not extract date from filename, defaulting to unknown/")
    return "unknown"


def write_json_to_s3(data, bucket, key, retries=3):
    for attempt in range(retries):
        try:
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(data, indent=2).encode("utf-8"),
                ContentType="application/json"
            )
            return
        except ClientError as e:
            logger.error(f"[RETRY] Failed to upload {key} (Attempt {attempt+1}): {str(e)}")
            if attempt == retries - 1:
                raise


def move_to_rejected(bucket, filename, content, reason):
    date_folder = extract_date_folder(filename)
    key = f"rejected/inventory/{date_folder}/{filename}"
    try:
        error_wrapper = {
            "error": f"File-level error: {reason}",
            "raw_content": content
        }
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(error_wrapper, indent=2).encode("utf-8"),
            ContentType="application/json"
        )
        logger.info(f"[REJECTED] File saved to {key} with reason: {reason}")
    except ClientError as e:
        logger.error(f"[FAIL] Could not upload rejected file: {str(e)}", exc_info=True)
