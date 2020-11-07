"""
Lambda functions integration tests.
"""

import json

import boto3

LAMBDA = boto3.client("lambda")


def test_should_launch_datasets_endpoint_lambda_function():
    """Test if datasets endpoint lambda can be successfully launched."""

    response = LAMBDA.invoke(FunctionName="datasets-endpoint", InvocationType="RequestResponse")
    json_response = json.loads(response["Payload"].read().decode("utf-8"))

    assert json_response["statusCode"] == 400
    assert json_response["body"]["message"] == "Bad Request: 'httpMethod' is a required property."
