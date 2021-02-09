"""
Dataset Versions endpoint Lambda function tests. Working Data Lake AWS environment is
required (run '$ cdk deploy' before running tests).
"""

import logging

import _pytest
from pytest import mark

from ..endpoints.dataset_versions import entrypoint
from .utils import Dataset, any_dataset_id, any_lambda_context, any_s3_url, any_valid_dataset_type

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_should_fail_if_request_not_containing_method() -> None:
    response = entrypoint.lambda_handler({"body": {}}, any_lambda_context())

    assert response == {
        "statusCode": 400,
        "body": {"message": "Bad Request: 'httpMethod' is a required property"},
    }


def test_should_fail_if_request_not_containing_body() -> None:
    response = entrypoint.lambda_handler({"httpMethod": "POST"}, any_lambda_context())

    assert response == {
        "statusCode": 400,
        "body": {"message": "Bad Request: 'body' is a required property"},
    }


def test_should_fail_if_post_request_not_containing_mandatory_attribute() -> None:
    # Given a missing "type" attribute in the body
    body = {}
    body["id"] = any_dataset_id()
    body["type"] = any_valid_dataset_type()

    # When attempting to create the instance
    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, any_lambda_context())

    # Then the API should return an error message
    assert response == {
        "statusCode": 400,
        "body": {"message": "Bad Request: 'metadata-url' is a required property"},
    }


def test_should_fail_if_post_request_not_containing_type() -> None:
    # Given a missing "type" attribute in the body
    body = {}
    body["id"] = any_dataset_id()
    body["metadata-url"] = any_s3_url()

    # When attempting to create the instance
    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, any_lambda_context())

    # Then the API should return an error message
    assert response == {
        "statusCode": 400,
        "body": {"message": "Bad Request: 'type' is a required property"},
    }


def test_should_fail_if_dataset_does_not_exist() -> None:
    body = {}
    body["id"] = any_dataset_id()
    body["metadata-url"] = any_s3_url()
    body["type"] = any_valid_dataset_type()

    response = entrypoint.lambda_handler({"httpMethod": "POST", "body": body}, any_lambda_context())
    logger.info("Response: %s", response)

    assert response == {
        "statusCode": 404,
        "body": {"message": f"Not Found: dataset '{body['id']}' could not be found"},
    }


@mark.infrastructure
def test_should_return_success_if_dataset_exists(
    db_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
) -> None:
    # Given a dataset instance
    dataset_id = any_dataset_id()
    dataset_type = any_valid_dataset_type()

    body = {}
    body["id"] = dataset_id
    body["metadata-url"] = any_s3_url()
    body["type"] = dataset_type

    with Dataset(dataset_id=dataset_id, dataset_type=dataset_type):
        # When requesting the dataset by ID and type
        response = entrypoint.lambda_handler(
            {"httpMethod": "POST", "body": body}, any_lambda_context()
        )
    logger.info("Response: %s", response)

    # Then we should get the dataset in return
    assert response["statusCode"] == 201
