"""
Data Lake stack tests.
"""

import os

import boto3
from pytest import mark

CF = boto3.client("cloudformation")


ENV = os.environ.get("DEPLOY_ENV", "dev")


@mark.infrastructure
def test_stack_create_complete():
    """Test if CloudFormation stack is successfully created."""
    response = CF.describe_stacks(StackName=f"geospatial-data-lake-{ENV}")
    assert response["Stacks"][0]["StackStatus"] in (
        "CREATE_COMPLETE",
        "UPDATE_COMPLETE",
    )
