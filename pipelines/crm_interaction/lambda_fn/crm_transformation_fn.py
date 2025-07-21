import json
import base64
import logging
import os
from datetime import datetime

# -------------------------------------
# Configure dynamic logging level
# -------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))


# -------------------------------------
# Lambda entrypoint function
# -------------------------------------
def lambda_handler(event, context):
    """
    Entry point for the AWS Lambda function.

    This function processes each record received from Kinesis Firehose:
    - Decodes base64-encoded JSON
    - Transforms and enriches data
    - Returns the result for each record (success/failure)
    """
    results = []

    for record in event.get('records', []):
        results.append(process_record(record))

    # Log a summary of processing results
    success = sum(1 for r in results if r['result'] == 'Ok')
    failure = len(results) - success
    logger.info(f"Processed: {success} success, {failure} failed")

    return {'records': results}


# -------------------------------------
# Process an individual Firehose record
# -------------------------------------
def process_record(record):
    try:
        raw_data = parse_base64_json(record['data'])           # Decode base64 and parse JSON
        transformed = transform_crm_record(raw_data)           # Apply transformation
        return build_success_response(record['recordId'], transformed)
    except Exception as e:
        logger.exception(f"Record {record.get('recordId')} failed. Error: {e}")
        return build_failure_response(record.get('recordId'))


# -------------------------------------
# Decode base64-encoded string and parse as JSON
# -------------------------------------
def parse_base64_json(encoded_data):
    decoded = base64.b64decode(encoded_data).decode('utf-8')
    return json.loads(decoded)


# -------------------------------------
# Transform raw CRM interaction record
# -------------------------------------
def transform_crm_record(data):
    # Validate required fields
    validate_required_fields(data, ['customer_id', 'interaction_type', 'timestamp'])

    # Return enriched and normalized record
    return {
        'customer_id': parse_int(data['customer_id']),
        'interaction_type': normalize_string(data['interaction_type']),
        'channel': normalize_optional_string(data.get('channel')),
        'rating': validate_rating(data.get('rating')),
        'message_excerpt': truncate_string(data.get('message_excerpt'), 1000),
        **derive_timestamp_fields(data['timestamp']),
        'processed_at': current_utc_timestamp(),
        'has_rating': is_valid_rating(data.get('rating')),
        'has_message': bool(data.get('message_excerpt')),
        'has_channel': bool(data.get('channel'))
    }


# -------------------------------------
# Ensure required fields are present
# -------------------------------------
def validate_required_fields(data, required_fields):
    for field in required_fields:
        if field not in data or data[field] is None:
            raise ValueError(f"Missing required field: {field}")


# -------------------------------------
# Parse and validate integer fields
# -------------------------------------
def parse_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid integer: {value}")


# -------------------------------------
# Normalize string by stripping and lowercasing
# -------------------------------------
def normalize_string(value):
    return str(value).strip().lower()


# -------------------------------------
# Normalize optional strings safely
# -------------------------------------
def normalize_optional_string(value):
    if value is None:
        return None
    try:
        return normalize_string(value)
    except Exception:
        logger.warning(f"Could not normalize string: {value}")
        return None


# -------------------------------------
# Truncate string fields to max length
# -------------------------------------
def truncate_string(value, max_length):
    if value is None:
        return None
    return str(value).strip()[:max_length]


# -------------------------------------
# Check if rating is a valid 1–5 integer
# -------------------------------------
def is_valid_rating(rating):
    try:
        rating_int = int(rating)
        return 1 <= rating_int <= 5
    except Exception:
        return False


# -------------------------------------
# Return rating if valid, else None
# -------------------------------------
def validate_rating(rating):
    return int(rating) if is_valid_rating(rating) else None


# -------------------------------------
# Convert timestamp to enriched fields
# -------------------------------------
def derive_timestamp_fields(timestamp):
    try:
        ts = float(timestamp)
        dt = datetime.utcfromtimestamp(ts)
        iso_ts = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'  # Trim to milliseconds
        return {
            'timestamp': iso_ts,
            'interaction_date': dt.date().isoformat(),
            'interaction_hour': dt.hour,
            'interaction_day_of_week': dt.weekday()
        }
    except Exception:
        raise ValueError("Invalid timestamp format")


# -------------------------------------
# Return current UTC timestamp (ISO 8601, milliseconds)
# -------------------------------------
def current_utc_timestamp():
    now = datetime.utcnow()
    return now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


# -------------------------------------
# Build successful response for Firehose
# -------------------------------------
def build_success_response(record_id, transformed):
    return {
        'recordId': record_id,
        'result': 'Ok',
        'data': base64.b64encode((json.dumps(transformed) + '\n').encode('utf-8')).decode('utf-8')
    }


# -------------------------------------
# Build failure response for Firehose
# -------------------------------------
def build_failure_response(record_id):
    return {
        'recordId': record_id,
        'result': 'ProcessingFailed'
    }
