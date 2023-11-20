import unittest.mock as mock

from main import _get_job_config

# Unit test for the get_job_config function
def test_get_job_config():
    # Given
    with mock.patch('builtins.open', mock.mock_open(read_data='{"key": "value"}')):
        # When
        result = _get_job_config('existing_file')
        # Then
        assert result == {'key': 'value'}


def validate_json(json_data, required_fields):
    return all(field in json_data for field in required_fields)

def test_validate_instance_pool_json():
    json = {
        "instance_pool_name": "auto_loader_batch_driver_banking_v3",
        "min_idle_instances": 0,
        "aws_attributes": {
            "availability": "ON_DEMAND",
            "zone_id": "us-east-1a",
            "spot_bid_price_percent": 100
        },
        "node_type_id": "c5d.4xlarge",
        "idle_instance_autotermination_minutes": 5,
        "enable_elastic_disk": False,
        "instance_pool_id": "1118-022651-soup459-pool-eshdk0r4",
        "default_tags": {
            "Vendor": "Databricks",
            "DatabricksInstancePoolCreatorId": "7446174290248091",
            "DatabricksInstancePoolId": "1118-022651-soup459-pool-eshdk0r4",
            "DatabricksInstanceGroupId": "-4900626866290173175"
        },
        "state": "ACTIVE",
        "stats": {
            "used_count": 0,
            "idle_count": 0,
            "pending_used_count": 0,
            "pending_idle_count": 0
        },
        "status": {}
    }
    required_fields = ["instance_pool_name", "min_idle_instances", "aws_attributes", "node_type_id"]

    assert validate_json(json, required_fields)        