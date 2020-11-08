"""
Lambda functions integration tests.
"""

import json
import uuid

import boto3

LAMBDA = boto3.client("lambda")


def test_should_launch_datasets_endpoint_lambda_function():
    """
    Test if datasets endpoint lambda can be successfully launched and has required permission to
    create dataset in DB.
    """

    method = "POST"
    body = {}
    body["type"] = "RASTER"
    body["title"] = f"Dataset {uuid.uuid1}.hex"
    body["owning_group"] = "A_ABC_XYZ"

    resp = LAMBDA.invoke(
        FunctionName="datasets-endpoint",
        Payload=json.dumps({"httpMethod": method, "body": body}),
        InvocationType="RequestResponse",
    )
    json_resp = json.loads(resp["Payload"].read().decode("utf-8"))

    assert json_resp["statusCode"] == 201
