"""
AWS infrastructure tests.
"""

import os

import boto3

CF = boto3.client("cloudformation")


if "ENVIRONMENT_TYPE" in os.environ:
    ENV = os.environ["ENVIRONMENT_TYPE"]
else:
    ENV = "dev"


def test_stack_create_complete():
    """Test if CloudFormation stack is successfully created."""
    response = CF.describe_stacks(StackName=f"geospatial-data-lake-{ENV}")
    assert response["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"
