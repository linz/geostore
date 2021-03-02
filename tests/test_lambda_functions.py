"""
Lambda functions integration tests.
"""

import json

from mypy_boto3_lambda import LambdaClient
from pytest import mark

from backend.utils import ResourceName

from .utils import any_dataset_owning_group, any_dataset_title, any_valid_dataset_type


@mark.infrastructure
def test_should_launch_datasets_endpoint_lambda_function(
    lambda_client: LambdaClient,
) -> None:
    """
    Test if datasets endpoint lambda can be successfully launched and has required permission to
    create dataset in DB.
    """

    method = "POST"
    body = {}
    body["type"] = any_valid_dataset_type()
    body["title"] = any_dataset_title()
    body["owning_group"] = any_dataset_owning_group()

    resp = lambda_client.invoke(
        FunctionName=ResourceName.DATASETS_ENDPOINT_FUNCTION_NAME.value,
        Payload=json.dumps({"httpMethod": method, "body": body}).encode(),
        InvocationType="RequestResponse",
    )
    json_resp = json.load(resp["Payload"])

    assert json_resp.get("statusCode") == 201, json_resp
