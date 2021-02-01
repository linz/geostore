"""
Lambda functions integration tests.
"""

import json
import uuid

from mypy_boto3_lambda import LambdaClient
from pytest import mark

from ..endpoints.utils import ResourceName


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
    body["type"] = "RASTER"
    body["title"] = f"Dataset {uuid.uuid1()}.hex"
    body["owning_group"] = "A_ABC_XYZ"

    resp = lambda_client.invoke(
        FunctionName=ResourceName.DATASETS_ENDPOINT_FUNCTION_NAME.value,
        Payload=json.dumps({"httpMethod": method, "body": body}).encode(),
        InvocationType="RequestResponse",
    )
    json_resp = json.loads(resp["Payload"].read().decode("utf-8"))

    assert json_resp.get("statusCode") == 201, json_resp
