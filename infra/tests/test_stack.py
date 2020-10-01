"""
AWS infrastructure tests.
"""

import boto3


CF = boto3.client("cloudformation")


def test_stack_create_complete():
    """Test if CloudFormation stack is successfully created."""
    response = CF.describe_stacks(StackName="geospatial-data-lake-dev")
    assert response["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"
