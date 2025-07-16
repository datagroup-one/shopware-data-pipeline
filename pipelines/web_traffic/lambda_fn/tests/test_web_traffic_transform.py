import json
import base64
import pytest
from datetime import datetime

from pipelines.web_traffic.lambda_fn.web_traffic_transformation_fn import (
    transform_web_traffic_record,
    build_success_response,
    build_failure_response,
    is_mobile_device,
    is_engagement_event,
    derive_page_fields,
    derive_timestamp_fields
)


# ------------------------
# Fixtures
# ------------------------

@pytest.fixture
def valid_event():
    return {
        "session_id": "ABC123",
        "user_id": "42",
        "page": "/products/item123",
        "timestamp": str(datetime.now().timestamp()),
        "device_type": "iPhone",
        "browser": "Safari",
        "event_type": "click"
    }

# ------------------------
# Transform Tests
# ------------------------

def test_transform_valid_event(valid_event):
    result = transform_web_traffic_record(valid_event)

    assert result["session_id"] == "abc123"
    assert result["user_id"] == 42
    assert result["page"] == "/products/item123"
    assert result["device_type"] == "iphone"
    assert result["browser"] == "safari"
    assert result["event_type"] == "click"
    assert "event_date" in result
    assert "processed_at" in result
    assert result["is_mobile"] is True
    assert result["is_engagement_event"] is True


def test_transform_missing_required_field():
    incomplete = {
        "page": "/home",
        "timestamp": str(datetime.now().timestamp())
    }

    with pytest.raises(ValueError, match="Missing required field: session_id"):
        transform_web_traffic_record(incomplete)


def test_transform_invalid_timestamp():
    bad_ts = {
        "session_id": "abc",
        "page": "/home",
        "timestamp": "not_a_ts"
    }

    with pytest.raises(ValueError, match="Invalid timestamp format"):
        transform_web_traffic_record(bad_ts)

# ------------------------
# Device Type Detection
# ------------------------

@pytest.mark.parametrize("device_type,expected", [
    ("iPhone", True),
    ("Tablet", True),
    ("Desktop", False),
    ("SmartTV", False),
    (None, False),
])
def test_is_mobile_device(device_type, expected):
    assert is_mobile_device(device_type) == expected

# ------------------------
# Engagement Type Detection
# ------------------------

@pytest.mark.parametrize("event_type,expected", [
    ("click", True),
    ("scroll", True),
    ("form_submit", True),
    ("logout", False),
    ("", False),
    (None, False),
])
def test_is_engagement_event(event_type, expected):
    assert is_engagement_event(event_type) == expected

# ------------------------
# Page Field Extraction
# ------------------------

def test_derive_page_fields_with_query():
    page = "/products/item/123?ref=ad"
    result = derive_page_fields(page)

    assert result["page_path"] == "/products/item/123"
    assert result["page_category"] == "product"
    assert result["path_depth"] == 3
    assert result["has_query_params"] is True


def test_derive_page_fields_with_unmapped_path():
    page = "/something/unexpected"
    result = derive_page_fields(page)

    assert result["page_category"] == "other"
    assert result["page_path"] == "/something/unexpected"
    assert result["path_depth"] == 2
    assert result["has_query_params"] is False


def test_derive_page_fields_with_root_path():
    result = derive_page_fields("/")
    assert result["page_category"] == "home"
    assert result["page_path"] == "/"
    assert result["path_depth"] == 0
    assert result["has_query_params"] is False


def test_derive_page_fields_with_malformed_input():
    result = derive_page_fields("%%%not/a/valid?url")
    assert result["page_category"] == "other"
    assert "page_path" in result
    assert result["path_depth"] >= 0

# ------------------------
# Timestamp Field Extraction
# ------------------------

def test_derive_timestamp_fields():
    dt = datetime(2024, 5, 10, 14, 30)
    ts = dt.timestamp()
    fields = derive_timestamp_fields(ts)

    assert fields["timestamp"] == dt.isoformat()
    assert fields["event_date"] == "2024-05-10"
    assert fields["event_hour"] == 14
    assert fields["event_day_of_week"] == 4  # Friday
    assert fields["year"] == 2024
    assert fields["month"] == 5
    assert fields["day"] == 10
    assert fields["hour"] == 14

# ------------------------
# Firehose Response Handling
# ------------------------

def test_build_success_response_encodes_data():
    payload = {"foo": "bar"}
    record = build_success_response("xyz", payload)

    assert record["recordId"] == "xyz"
    assert record["result"] == "Ok"

    decoded = json.loads(base64.b64decode(record["data"]).decode("utf-8"))
    assert decoded["foo"] == "bar"


def test_build_failure_response_structure():
    record = build_failure_response("abc")
    assert record == {
        "recordId": "abc",
        "result": "ProcessingFailed"
    }
