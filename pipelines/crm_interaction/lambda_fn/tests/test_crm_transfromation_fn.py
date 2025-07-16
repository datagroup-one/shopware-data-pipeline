import pytest
from pipelines.crm_interaction.lambda_fn.crm_transformation_fn import transform_crm_record


def test_transform_crm_record_valid():
    input_record = {
        "customer_id": "101",
        "interaction_type": "CHAT",
        "timestamp": "1721138041.0",
        "channel": "Mobile",
        "rating": "5",
        "message_excerpt": "Very helpful!"
    }

    result = transform_crm_record(input_record)

    assert result["customer_id"] == 101
    assert result["interaction_type"] == "chat"
    assert result["rating"] == 5
    assert result["has_rating"] is True
    assert result["has_message"] is True
    assert result["has_channel"] is True
    assert result["interaction_hour"] >= 0 and result["interaction_hour"] <= 23
