"""
Lambda functions integration tests.
"""

import json

from mypy_boto3_lambda import LambdaClient
from pytest import mark

from ..processing.utils import ResourceName
from .utils import (
    Dataset,
    any_dataset_id,
    any_dataset_owning_group,
    any_dataset_title,
    any_s3_url,
    any_valid_dataset_type,
)


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


@mark.infrastructure
def test_should_launch_dataset_versions_endpoint_lambda_function(
    lambda_client: LambdaClient,
) -> None:
    """
    Test if dataset versions endpoint lambda can be successfully launched
    and has required permission to access dataset DB and launch step function
    """

    method = "POST"
    body = {}
    body["type"] = dataset_type = any_valid_dataset_type()
    body["id"] = dataset_id = any_dataset_id()
    body["metadata-url"] = any_s3_url()

    with Dataset(dataset_id=dataset_id, dataset_type=dataset_type):

        resp = lambda_client.invoke(
            FunctionName=ResourceName.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.value,
            Payload=json.dumps({"httpMethod": method, "body": body}).encode(),
            InvocationType="RequestResponse",
        )
        json_resp = json.load(resp["Payload"])

        assert json_resp.get("statusCode") == 201, json_resp
