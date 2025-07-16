import pytest
from pipelines.crm_interaction.lambda_fn.crm_transform import transform_crm_record

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
