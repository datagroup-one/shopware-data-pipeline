import boto3
import csv
import io
import logging
import re
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

REQUIRED_FIELDS = {
    "transaction_id": str,
    "store_id": int,
    "product_id": int,
    "quantity": int,
    "revenue": float,
    "timestamp": float,
}

OPTIONAL_FIELDS = {
    "discount_applied": float,
}

EXPECTED_HEADERS = list(REQUIRED_FIELDS.keys()) + list(OPTIONAL_FIELDS.keys())

def validate_row(row):
    for field, cast_type in REQUIRED_FIELDS.items():
        if not row.get(field):
            return False, f"Missing required field: {field}"
        try:
            cast_type(row[field])
        except ValueError:
            return False, f"Invalid type for field: {field}"
    if row.get("discount_applied"):
        try:
            float(row["discount_applied"])
        except ValueError:
            return False, "Invalid discount_applied"
    return True, None


def lambda_handler(event, context):
    for record in event['Records']:
        try:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            filename = key.split("/")[-1]
            logger.info(f"Processing file: s3://{bucket}/{key}")

            response = s3.get_object(Bucket=bucket, Key=key)
            file_content = response['Body'].read().decode('utf-8')
            reader = csv.DictReader(io.StringIO(file_content))

            header_fields = reader.fieldnames
            missing_fields = [f for f in REQUIRED_FIELDS if f not in header_fields]
            extra_fields = [f for f in header_fields if f not in EXPECTED_HEADERS]

            if missing_fields:
                reason = f"Header mismatch: Missing required fields {missing_fields}"
                logger.warning(f"{reason} in file: {key}. Extra fields: {extra_fields}")
                move_to_rejected(bucket, filename, file_content, reason)
                continue

            valid_rows, invalid_rows = [], []
            for row in reader:
                is_valid, reason = validate_row(row)
                if is_valid:
                    valid_rows.append(row)
                else:
                    row["rejection_reason"] = reason
                    invalid_rows.append(row)

            date_folder = extract_date_folder(filename)

            if valid_rows:
                raw_key = f"raw-data/pos/{date_folder}/{filename}"
                write_csv_to_s3(valid_rows, EXPECTED_HEADERS, bucket, raw_key)
                logger.info(f"{len(valid_rows)} valid rows saved to {raw_key}")

            if invalid_rows:
                rejected_key = f"rejected/pos/{date_folder}/{filename}"
                headers_with_reason = EXPECTED_HEADERS + ["rejection_reason"]
                write_csv_to_s3(invalid_rows, headers_with_reason, bucket, rejected_key)
                logger.warning(f"{len(invalid_rows)} invalid rows saved to {rejected_key}")

        except Exception as e:
            logger.error(f"Unexpected error while processing file {key}: {str(e)}", exc_info=True)

def extract_date_folder(filename):
    match = re.search(r'pos_(\d{8})_', filename)
    if match:
        date_str = match.group(1)
        year, month, day = date_str[:4], date_str[4:6], date_str[6:]
        return f"{year}/{month}/{day}"
    else:
        logger.warning("Could not parse date from filename, defaulting to unknown/")
        return "unknown"

def write_csv_to_s3(rows, headers, bucket, target_key, retries=3):
    for attempt in range(retries):
        try:
            buffer = io.StringIO()
            writer = csv.DictWriter(buffer, fieldnames=headers)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
            s3.put_object(Bucket=bucket, Key=target_key, Body=buffer.getvalue().encode('utf-8'))
            return
        except ClientError as e:
            logger.error(f"Failed to upload {target_key} (attempt {attempt+1}): {str(e)}")
            if attempt == retries - 1:
                raise

def move_to_rejected(bucket, filename, content, reason):
    date_folder = extract_date_folder(filename)
    rejected_key = f"rejected/pos/{date_folder}/{filename}"
    content_lines = content.splitlines()
    if content_lines:
        content_lines[0] += ",rejection_reason"
        for i in range(1, len(content_lines)):
            content_lines[i] += f",File-level error: {reason}"
    new_content = "\n".join(content_lines)

    try:
        s3.put_object(Bucket=bucket, Key=rejected_key, Body=new_content.encode("utf-8"))
        logger.info(f"File with header issue saved to {rejected_key}")
    except ClientError as e:
        logger.error(f"Failed to upload rejected file: {str(e)}", exc_info=True)
