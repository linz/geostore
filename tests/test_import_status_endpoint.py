"""
Dataset Versions endpoint Lambda function tests.
"""
import logging
from unittest.mock import MagicMock, patch

from backend.import_status import entrypoint

from .general_generators import any_valid_arn
from .aws_utils import any_lambda_context


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
    response = entrypoint.lambda_handler({"httpMethod": "GET", "body": {}}, any_lambda_context())

    # Then the API should return an error message
    assert response == {
        "statusCode": 400,
        "body": {"message": "Bad Request: 'execution_arn' is a required property"},
    }


@patch("backend.import_status.get.STEPFUNCTIONS_CLIENT.describe_execution")
def test_should_report_upload_as_pending_when_validation_underway(
    describe_execution_mock: MagicMock,
) -> None:
    describe_execution_mock.return_value = {"status": "RUNNING"}

    req_body = {"execution_arn": any_valid_arn()}

    # When attempting to create the instance
    response = entrypoint.lambda_handler(
        {"httpMethod": "GET", "body": req_body}, any_lambda_context()
    )

    assert response["statusCode"] == 200, response
    assert response["body"]["validation_status"] == "RUNNING"
    assert response["body"]["upload_status"] == "Pending"


@patch("backend.import_status.get.STEPFUNCTIONS_CLIENT.describe_execution")
@patch("backend.import_status.get.S3CONTROL_CLIENT.describe_job")
def test_should_report_upload_as_pending_when_validation_test(
    describe_execution_mock: MagicMock,
    # describe_s3_job_mock: MagicMock,
) -> None:
    describe_execution_mock.return_value = {"status": "SUCCEEDED"}

    req_body = {"execution_arn": any_valid_arn()}

    # When attempting to create the instance
    response = entrypoint.lambda_handler(
        {"httpMethod": "GET", "body": req_body}, any_lambda_context()
    )

    assert response["statusCode"] == 200, response
    assert response["body"]["validation_status"] == "RUNNING"
    # assert describe_s3_job_mock.assert_has_calls([call()])
    assert response["body"]["upload_status"] == "Pending"
