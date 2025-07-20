import json
import base64
import logging
import os
from datetime import datetime
from urllib.parse import urlparse

# Configure logging level from environment variable (default: INFO)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))


def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    Processes a list of records and returns transformed results.
    """
    results = []

    for record in event.get('records', []):
        results.append(process_record(record))

    logger.info(f"Processed: {sum(1 for r in results if r['result'] == 'Ok')} success, "
                f"{sum(1 for r in results if r['result'] != 'Ok')} failed")

    return {'records': results}


def process_record(record):
    """
    Parses and transforms a single record.
    Returns a success or failure response.
    """
    try:
        raw_data = parse_base64_json(record['data'])
        transformed = transform_record(raw_data)
        return build_success_response(record['recordId'], transformed)
    except Exception as e:
        logger.exception(f"Failed to process record {record['recordId']}")
        return build_failure_response(record['recordId'])


def parse_base64_json(encoded_data):
    """
    Decodes base64-encoded JSON string into a Python dictionary.
    """
    decoded = base64.b64decode(encoded_data).decode('utf-8')
    return json.loads(decoded)


def transform_record(data):
    """
    Applies data validation, enrichment, and transformation to an input record.
    """
    # Check that required fields are present
    validate_required_fields(data, ['session_id', 'page', 'timestamp'])

    # Add enriched timestamp fields
    ts_fields = enrich_timestamp(data['timestamp'])

    # Add page-level metadata
    page_info = enrich_page_info(data['page'])

    # Return combined enriched record
    return {
        'session_id': str(data['session_id']).strip().lower(),
        'user_id': parse_optional_int(data.get('user_id')),
        'page': str(data['page']).strip().lower(),
        'device_type': optional_lower(data.get('device_type')),
        'browser': optional_lower(data.get('browser')),
        'event_type': optional_lower(data.get('event_type')),
        **ts_fields,
        **page_info,
        'processed_at': datetime.utcnow().isoformat(),
        'is_authenticated': data.get('user_id') is not None,
        'is_mobile': is_mobile(data.get('device_type')),
        'is_engagement_event': is_engagement_event(data.get('event_type'))
    }


def validate_required_fields(data, fields):
    """
    Ensures all required fields are present and not None.
    """
    for field in fields:
        if field not in data or data[field] is None:
            raise ValueError(f"Missing required field: {field}")


def parse_optional_int(value):
    """
    Converts value to int if not None.
    """
    if value is None:
        return None
    return int(value)


def optional_lower(value):
    """
    Converts string to lowercase if not None.
    """
    return str(value).strip().lower() if value else None


def is_mobile(device_type):
    """
    Determines whether the device type is considered mobile.
    """
    if not device_type:
        return False
    return any(keyword in device_type.lower() for keyword in ['mobile', 'tablet', 'android', 'ios', 'iphone', 'ipad'])


def is_engagement_event(event_type):
    """
    Determines whether the event type indicates user engagement.
    """
    if not event_type:
        return False
    return event_type.lower() in ['click', 'scroll', 'form_submit', 'download', 'video_play', 'share']


def enrich_timestamp(ts):
    """
    Parses timestamp and returns multiple time-based attributes.
    """
    dt = datetime.fromtimestamp(float(ts))
    return {
        'timestamp': dt.strftime('%Y-%m-%d %H:%M:%S'),
        'timestamp_iso': dt.isoformat(),
        'event_date': dt.date().isoformat(),
        'event_hour': dt.hour,
        'event_day_of_week': dt.weekday(),
        'year': dt.year,
        'month': dt.month,
        'day': dt.day,
        'hour': dt.hour
    }


def enrich_page_info(page):
    """
    Parses and classifies page URL/path.
    Extracts category and metadata such as path depth and query presence.
    """
    try:
        parsed = urlparse(page if page.startswith('http') else f'https://example.com{page}')
        path_parts = [p for p in parsed.path.split('/') if p]
        return {
            'page_path': parsed.path,
            'page_category': map_page_category(path_parts),
            'path_depth': len(path_parts),
            'has_query_params': bool(parsed.query)
        }
    except:
        return {
            'page_path': page,
            'page_category': 'unknown',
            'path_depth': 0,
            'has_query_params': False
        }


def map_page_category(path_parts):
    """
    Maps the first segment of the URL path to a high-level category.
    """
    if not path_parts:
        return 'home'
    first = path_parts[0].lower()
    return {
        'product': 'product', 'products': 'product', 'item': 'product',
        'cart': 'commerce', 'checkout': 'commerce', 'order': 'commerce',
        'account': 'account', 'profile': 'account',
        'login': 'auth', 'register': 'auth', 'signup': 'auth',
        'about': 'content', 'contact': 'content', 'support': 'support',
        'help': 'support', 'blog': 'content', 'news': 'content',
        'search': 'search', 'category': 'navigation',
        'admin': 'admin', 'api': 'api'
    }.get(first, 'other')


def build_success_response(record_id, transformed):
    """
    Builds a success response for a successfully processed record.
    Includes partitioning metadata.
    """
    return {
        'recordId': record_id,
        'result': 'Ok',
        'data': base64.b64encode((json.dumps(transformed) + '\n').encode('utf-8')).decode('utf-8'),
        'recordMetadata': {
            'partitionKeys': {
                'event_date': transformed['event_date']
            }
        }
    }


def build_failure_response(record_id):
    """
    Builds a failure response for a record that could not be processed.
    """
    return {
        'recordId': record_id,
        'result': 'ProcessingFailed'
    }
