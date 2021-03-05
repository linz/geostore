"""
Dataset Versions endpoint Lambda function tests.
"""

import logging
from unittest.mock import MagicMock, patch

from pytest import mark

from backend.import_status import entrypoint

from .utils import any_lambda_context, any_s3_url

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_should_return_required_property_error_when_missing_http_method() -> None:
    response = entrypoint.lambda_handler({"body": {}}, any_lambda_context())

    assert response == {
        "statusCode": 400,
        "body": {"message": "Bad Request: 'httpMethod' is a required property"},
    }


def test_should_return_required_property_error_when_missing_body() -> None:
    response = entrypoint.lambda_handler({"httpMethod": "GET"}, any_lambda_context())

    assert response == {
        "statusCode": 400,
        "body": {"message": "Bad Request: 'body' is a required property"},
    }


def test_should_return_required_property_error_when_missing_mandatory_execution_arn() -> None:
    # Given a missing "execution_arn" attribute in the body
    body = {"execution_arn": any_s3_url()}

    # When attempting to create the instance
    response = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, any_lambda_context())

    # Then the API should return an error message
    assert response == {
        "statusCode": 400,
        "body": {"message": "Bad Request: 'execution_arn' is a required property"},
    }


@mark.infrastructure
@patch("backend.dataset_versions.create.stepfunctions_client.describe_execution")
def test_should_return_success_if_dataset_exists(
    describe_execution_mock: MagicMock,  # pylint:disable=unused-argument
) -> None:

    describe_execution_mock.return_value = {"status": "FAILED"}
    body = {"execution_arn": any_s3_url()}

    response = entrypoint.lambda_handler({"httpMethod": "GET", "body": body}, any_lambda_context())

    logger.info("Response: %s", response)

    # Then we should get the dataset in return
    assert response["statusCode"] == 200
