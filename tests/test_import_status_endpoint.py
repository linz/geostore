"""
Dataset Versions endpoint Lambda function tests.
"""
import json
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
def test_should_report_upload_status_as_pending_when_validation_underway(
    describe_execution_mock: MagicMock,
) -> None:
    describe_execution_mock.return_value = {"status": "RUNNING"}

    # When attempting to create the instance
    response = entrypoint.lambda_handler(
        {"httpMethod": "GET", "body": {"execution_arn": any_valid_arn()}}, any_lambda_context()
    )

    assert response["statusCode"] == 200, response
    assert response["body"]["validation"]["status"] == "RUNNING"
    assert response["body"]["upload"]["status"] == "Pending"


@patch("backend.import_status.get.STEPFUNCTIONS_CLIENT.describe_execution")
@patch("backend.import_status.get.S3CONTROL_CLIENT.describe_job")
def test_should_report_upload_failures(
    describe_s3_job_mock: MagicMock,
    describe_execution_mock: MagicMock,
) -> None:
    describe_execution_mock.return_value = {
        "status": "SUCCEEDED",
        "output": json.dumps({"s3_batch_copy": {"job_id": "test"}}),
    }

    describe_s3_job_mock.return_value = {
        "Job": {
            "Status": "Completed",
            "FailureReasons": [{"FailureCode": "TEST_CODE", "FailureReason": "TEST_REASON"}],
        }
    }

    with patch("backend.import_status.get.STS_CLIENT.get_caller_identity"):

        response = entrypoint.lambda_handler(
            {"httpMethod": "GET", "body": {"execution_arn": any_valid_arn()}}, any_lambda_context()
        )

        assert response["statusCode"] == 200, response
        assert response["body"]["validation"]["status"] == "SUCCEEDED"
        assert response["body"]["upload"]["status"] == "Completed"
        assert response["body"]["upload"]["errors"][0]["FailureCode"] == "TEST_CODE"
        assert response["body"]["upload"]["errors"][0]["FailureReason"] == "TEST_REASON"
