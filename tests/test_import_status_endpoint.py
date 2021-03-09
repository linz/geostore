"""
Dataset Versions endpoint Lambda function tests.
"""
import logging
from unittest.mock import MagicMock, patch

from backend.import_status import entrypoint
from backend.import_status.get import get_s3_batch_copy_status

from .aws_utils import any_arn_formatted_string, any_lambda_context


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


@patch("backend.import_status.get.STEP_FUNCTIONS_CLIENT.describe_execution")
def test_should_report_upload_status_as_pending_when_validation_incomplete(
    describe_execution_mock: MagicMock,
) -> None:
    describe_execution_mock.return_value = {"status": "RUNNING"}

    expected_response = {
        "statusCode": 200,
        "body": {
            "validation": {"status": "RUNNING"},
            "upload": {"status": "Pending", "errors": []},
        },
    }

    # When attempting to create the instance
    response = entrypoint.lambda_handler(
        {"httpMethod": "GET", "body": {"execution_arn": any_arn_formatted_string()}},
        any_lambda_context(),
    )

    assert response == expected_response


class TestsWithLogger:
    logger: logging.Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logging.getLogger("backend.import_status.get")

    @patch("backend.import_status.get.S3CONTROL_CLIENT.describe_job")
    def test_should_report_upload_failures(
        self,
        describe_s3_job_mock: MagicMock,
    ) -> None:
        describe_s3_job_mock.return_value = {
            "Job": {
                "Status": "Completed",
                "FailureReasons": [{"FailureCode": "TEST_CODE", "FailureReason": "TEST_REASON"}],
            }
        }

        expected_response = {
            "status": "Pending",
            "errors": [{"FailureCode": "TEST_CODE", "FailureReason": "TEST_ERASON"}],
        }

        with patch("backend.import_status.get.STS_CLIENT.get_caller_identity"):
            s3_batch_response = get_s3_batch_copy_status(
                "test",
                self.logger,
            )

            assert s3_batch_response == expected_response
