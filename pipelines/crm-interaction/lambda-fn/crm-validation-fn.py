import json
import gzip
import boto3
import logging
import os
from datetime import datetime
from urllib.parse import unquote_plus

# Initialize
s3 = boto3.client("s3")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Expected S3 bucket name only
VALIDATED_BUCKET = os.environ.get("VALIDATED_BUCKET", "shopware.bucket")

# Validation function
def is_valid_crm_record(record_str):
    try:
        record = json.loads(record_str)

        if not isinstance(record.get("customer_id"), int):
            return False
        if not isinstance(record.get("interaction_type"), str) or not record["interaction_type"].strip():
            return False
        if not isinstance(record.get("timestamp"), (int, float)):
            return False

        if "channel" in record and not isinstance(record["channel"], str):
            return False
        if "rating" in record and (not isinstance(record["rating"], int) or not (1 <= record["rating"] <= 5)):
            return False
        if "message_excerpt" in record and not isinstance(record["message_excerpt"], str):
            return False

        return True
    except Exception as e:
        logger.error(f"Validation error: {e}")
        return False

# Enrichment function
def enrich_crm_record(record_str, source_ip):
    try:
        record = json.loads(record_str)
        record["ingestion_time"] = datetime.utcnow().isoformat()
        record["source_ip"] = source_ip
        return record
    except Exception as e:
        logger.error(f"Enrichment error: {e}")
        return None

# S3 writer utility
def write_records_to_s3(records, original_key, sub_path):
    if not records:
        return

    now = datetime.utcnow()
    date_prefix = f"{now.year}/{now.month:02}/{now.day:02}"
    filename = os.path.basename(original_key).replace(".gz", "")
    s3_key = f"curated-data/crm-interaction-streams/{sub_path}/{date_prefix}/{filename}"
    body = "\n".join(json.dumps(r) for r in records)

    try:
        s3.put_object(
            Bucket=VALIDATED_BUCKET,
            Key=s3_key,
            Body=body.encode("utf-8"),
            ContentType="application/json"
        )
        logger.info(f"Wrote {len(records)} records to s3://{VALIDATED_BUCKET}/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to write to s3://{VALIDATED_BUCKET}/{s3_key}: {e}")

# Lambda entrypoint
def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    for s3_event in event.get("Records", []):
        bucket = s3_event["s3"]["bucket"]["name"]
        key = unquote_plus(s3_event["s3"]["object"]["key"])
        source_ip = s3_event.get("requestParameters", {}).get("sourceIPAddress", "unknown")

        try:
            response = s3.get_object(Bucket=bucket, Key=key)

            if key.endswith(".gz"):
                lines = gzip.decompress(response["Body"].read()).decode("utf-8").splitlines()
            else:
                lines = response["Body"].read().decode("utf-8").splitlines()

            valid_records = []
            invalid_records = []

            for line in lines:
                if is_valid_crm_record(line):
                    enriched = enrich_crm_record(line, source_ip)
                    if enriched:
                        valid_records.append(enriched)
                else:
                    invalid_records.append({"raw": line})

            logger.info(f"{key} | Valid: {len(valid_records)} | Invalid: {len(invalid_records)}")

            write_records_to_s3(valid_records, key, "valid")
            write_records_to_s3(invalid_records, key, "invalid")

        except Exception as e:
            logger.error(f"Error processing {key} from bucket {bucket}: {e}")

    return {
        "statusCode": 200,
        "body": json.dumps("CRM record validation and enrichment complete.")
    }
