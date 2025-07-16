import json
import base64
import logging
import os
from datetime import datetime
from urllib.parse import urlparse

# -------------------------------------
# Configure dynamic logging level
# -------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# --------------------------
# Lambda entrypoint function
# --------------------------
def lambda_handler(event, context):
    """
    AWS Lambda entry point for transforming Web Traffic Log records from Firehose.

    Processes each record:
    - Decodes Base64
    - Parses JSON
    - Validates and transforms fields
    - Returns enriched record to Firehose

    Supports KPIs: Engagement Scores, Session Metrics, Loyalty Activity
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
        transformed = transform_web_traffic_record(raw_data)   # Transform record
        return build_success_response(record['recordId'], transformed)
    except Exception as e:
        logger.exception(f"Record {record['recordId']} failed. Raw data: {record.get('data')}")
        return build_failure_response(record['recordId'])


# Decode Base64-encoded string to JSON object
# --------------------------------------------
def parse_base64_json(encoded_data):
    decoded = base64.b64decode(encoded_data).decode('utf-8')
    return json.loads(decoded)


# Main transformation logic for Web Traffic Log record
# ----------------------------------------------------
def transform_web_traffic_record(data):
    # Ensure required fields are present
    validate_required_fields(data, ['session_id', 'page', 'timestamp'])

    # Construct the output record
    return {
        'session_id': normalize_string(data['session_id']),
        'user_id': parse_optional_int(data.get('user_id')),
        'page': normalize_string(data['page']),
        'device_type': normalize_optional_string(data.get('device_type')),
        'browser': normalize_optional_string(data.get('browser')),
        'event_type': normalize_optional_string(data.get('event_type')),
        **derive_timestamp_fields(data['timestamp']),
        **derive_page_fields(data['page']),
        'processed_at': datetime.utcnow().isoformat(),
        'is_authenticated': bool(data.get('user_id')),
        'is_mobile': is_mobile_device(data.get('device_type')),
        'is_engagement_event': is_engagement_event(data.get('event_type'))
    }


# Required field validator
# -------------------------------------
def validate_required_fields(data, required_fields):
    for field in required_fields:
        if field not in data or data[field] is None:
            raise ValueError(f"Missing required field: {field}")


# Parse optional integer fields (handles null user_id)
# -------------------------------------
def parse_optional_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid integer: {value}")


# Normalize string fields (lowercase + trim)
# -------------------------------------
def normalize_string(value):
    return str(value).strip().lower()


# Normalize optional string fields with fallback logging
# -------------------------------------
def normalize_optional_string(value):
    if value is None:
        return None
    try:
        return normalize_string(value)
    except Exception:
        logger.warning(f"Could not normalize value: {value}")
        return None


# Check if device is mobile for KPI analysis
# -------------------------------------
def is_mobile_device(device_type):
    if not device_type:
        return False
    mobile_types = ['mobile', 'tablet', 'android', 'ios', 'iphone', 'ipad']
    return any(mobile in device_type.lower() for mobile in mobile_types)


# Check if event type indicates engagement for scoring
# -------------------------------------
def is_engagement_event(event_type):
    if not event_type:
        return False
    engagement_events = ['click', 'scroll', 'form_submit', 'download', 'video_play', 'share']
    return event_type.lower() in engagement_events


# Extract and enrich time-based fields
# -------------------------------------
def derive_timestamp_fields(timestamp):
    try:
        ts = float(timestamp)
        dt = datetime.fromtimestamp(ts)
        return {
            'timestamp': dt.isoformat(),                # Full timestamp
            'event_date': dt.date().isoformat(),        # YYYY-MM-DD
            'event_hour': dt.hour,                      # 0-23
            'event_day_of_week': dt.weekday(),          # 0=Monday
            'year': dt.year,
            'month': dt.month,
            'day': dt.day,
            'hour': dt.hour
        }
    except Exception:
        raise ValueError("Invalid timestamp format")


# Extract page-related fields for analytics
# -------------------------------------
def derive_page_fields(page):
    try:
        parsed = urlparse(page if page.startswith('http') else f'https://example.com{page}')
        path_parts = [p for p in parsed.path.split('/') if p]
        page_category = determine_page_category(path_parts)

        return {
            'page_path': parsed.path,
            'page_category': page_category,
            'path_depth': len(path_parts),
            'has_query_params': bool(parsed.query)
        }
    except Exception:
        return {
            'page_path': page,
            'page_category': 'unknown',
            'path_depth': 0,
            'has_query_params': False
        }


# Categorize pages for analytics
# -------------------------------------
def determine_page_category(path_parts):
    if not path_parts:
        return 'home'

    first_part = path_parts[0].lower()

    category_map = {
        'product': 'product',
        'products': 'product',
        'item': 'product',
        'cart': 'commerce',
        'checkout': 'commerce',
        'order': 'commerce',
        'account': 'account',
        'profile': 'account',
        'login': 'auth',
        'register': 'auth',
        'signup': 'auth',
        'about': 'content',
        'contact': 'content',
        'support': 'support',
        'help': 'support',
        'blog': 'content',
        'news': 'content',
        'search': 'search',
        'category': 'navigation',
        'admin': 'admin',
        'api': 'api'
    }

    return category_map.get(first_part, 'other')


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
