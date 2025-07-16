import json
import base64
import logging
from datetime import datetime

# Configure basic logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --------------------------
# Lambda entrypoint function
# --------------------------
def lambda_handler(event, context):
    """
    AWS Lambda entry point for transforming CRM interaction records from Firehose.

    Processes each record:
    - Decodes Base64
    - Parses JSON
    - Validates and transforms fields
    - Returns enriched record to Firehose

    Supports KPIs: Feedback Scores, Interaction Volume, Resolution Time, Loyalty Activity
    """
    results = []

    for record in event.get('records', []):
        results.append(process_record(record))

    # Log summary
    success = sum(1 for r in results if r['result'] == 'Ok')
    failure = len(results) - success
    logger.info(f"Processed: {success} success, {failure} failed")

    return {'records': results}


# Process individual record
# -------------------------
def process_record(record):
    try:
        raw_data = parse_base64_json(record['data'])           # Decode & parse JSON
        transformed = transform_crm_record(raw_data)           # Transform record
        return build_success_response(record['recordId'], transformed)
    except Exception as e:
        logger.error(f"Record {record['recordId']} failed: {e}")
        return build_failure_response(record['recordId'])


# Decode Base64-encoded string to JSON object
# --------------------------------------------
def parse_base64_json(encoded_data):
    decoded = base64.b64decode(encoded_data).decode('utf-8')
    return json.loads(decoded)


# Main transformation logic for CRM interaction record
# ----------------------------------------------------
def transform_crm_record(data):
    # Ensure required fields are present
    validate_required_fields(data, ['customer_id', 'interaction_type', 'timestamp'])

    # Construct the output record
    return {
        'customer_id': parse_int(data['customer_id']),                         # Normalize customer ID
        'interaction_type': normalize_string(data['interaction_type']),        # Lowercase interaction type
        'channel': normalize_optional_string(data.get('channel')),             # Optional communication channel
        'rating': validate_rating(data.get('rating')),                         # Validate rating (1–5)
        'message_excerpt': truncate_string(data.get('message_excerpt'), 1000), # Shorten long messages
        **derive_timestamp_fields(data['timestamp']),                          # Add interaction_date, hour, weekday
        'processed_at': datetime.utcnow().isoformat(),                         # Record processing time
        'has_rating': is_valid_rating(data.get('rating')),                     # Bool flag for KPI filtering
        'has_message': bool(data.get('message_excerpt')),                      # Bool flag for analytics
        'has_channel': bool(data.get('channel'))                               # Bool flag for data completeness
    }


# Required field validator
# -------------------------------------
def validate_required_fields(data, required_fields):
    for field in required_fields:
        if field not in data or data[field] is None:
            raise ValueError(f"Missing required field: {field}")


# Parse and validate integer fields
# -------------------------------------
def parse_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid integer: {value}")


# Normalize string fields (lowercase + trim)
# -------------------------------------
def normalize_string(value):
    return str(value).strip().lower()


# Normalize optional string fields
# -------------------------------------
def normalize_optional_string(value):
    return normalize_string(value) if value else None


# Truncate long strings
# -------------------------------------
def truncate_string(value, max_length):
    return str(value).strip()[:max_length] if value else None


# Validate if rating is in 1–5 range
# -------------------------------------
def is_valid_rating(rating):
    try:
        return 1 <= int(rating) <= 5
    except Exception:
        return False

def validate_rating(rating):
    return int(rating) if is_valid_rating(rating) else None


# Extract and enrich time-based fields
# -------------------------------------
def derive_timestamp_fields(timestamp):
    try:
        ts = float(timestamp)
        dt = datetime.fromtimestamp(ts)
        return {
            'timestamp': dt.isoformat(),                 # Full timestamp (ISO)
            'interaction_date': dt.date().isoformat(),   # For grouping by day
            'interaction_hour': dt.hour,                 # For time-of-day analysis
            'interaction_day_of_week': dt.weekday()      # 0 = Monday, 6 = Sunday
        }
    except Exception:
        raise ValueError("Invalid timestamp format")


# Build success Firehose record
# -------------------------------------
def build_success_response(record_id, transformed):
    return {
        'recordId': record_id,
        'result': 'Ok',
        'data': base64.b64encode((json.dumps(transformed) + '\n').encode('utf-8')).decode('utf-8')
    }

# Build failure Firehose record
# -------------------------------------
def build_failure_response(record_id):
    return {
        'recordId': record_id,
        'result': 'ProcessingFailed'
    }
