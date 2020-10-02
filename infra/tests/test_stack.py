"""
AWS infrastructure tests.
"""

import os

import boto3

CF = boto3.client("cloudformation")


if "ENVIRONMENT_TYPE" in os.environ:
    env = os.environ["ENVIRONMENT_TYPE"]
else:
    env = "dev"


def test_stack_create_complete():
    """Test if CloudFormation stack is successfully created."""
    response = CF.describe_stacks(StackName=f"geospatial-data-lake-{env}")
    assert response["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"
