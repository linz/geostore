"""
Data Lake stack tests.
"""

import os

import boto3

CF = boto3.client("cloudformation")


if "DEPLOY_ENV" in os.environ:
    ENV = os.environ["DEPLOY_ENV"]
else:
    ENV = "dev"


def test_stack_create_complete():
    """Test if CloudFormation stack is successfully created."""
    response = CF.describe_stacks(StackName=f"geospatial-data-lake-{ENV}")
    assert response["Stacks"][0]["StackStatus"] in (
        "CREATE_COMPLETE",
        "UPDATE_COMPLETE",
    )
