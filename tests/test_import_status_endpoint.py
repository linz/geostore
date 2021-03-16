"""
Dataset Versions endpoint Lambda function tests.
"""
import json
from unittest.mock import MagicMock, patch

from pytest import mark

from backend.import_status import entrypoint
from backend.import_status.get import get_step_function_validation_results
from backend.validation_results_model import ValidationResult

from .aws_utils import (
    ValidationItem,
    any_arn_formatted_string,
    any_job_id,
    any_lambda_context,
    any_s3_url,
)
from .stac_generators import any_dataset_id, any_dataset_version_id


def should_return_required_property_error_when_missing_mandatory_execution_arn() -> None:
    # Given a missing "execution_arn" attribute in the body
    response = entrypoint.lambda_handler({"httpMethod": "GET", "body": {}}, any_lambda_context())

    # Then the API should return an error message
    assert response == {
        "statusCode": 400,
        "body": {"message": "Bad Request: 'execution_arn' is a required property"},
    }


@patch("backend.import_status.get.STEP_FUNCTIONS_CLIENT.describe_execution")
def should_report_upload_status_as_pending_when_validation_incomplete(
    describe_execution_mock: MagicMock,
) -> None:
    # Given
    describe_execution_mock.return_value = {
        "status": "RUNNING",
        "input": json.dumps(
            {"dataset_id": any_dataset_id(), "version_id": any_dataset_version_id()}
        ),
    }

    expected_response = {
        "statusCode": 200,
        "body": {
            "validation": {"status": "RUNNING", "errors": []},
            "upload": {"status": "Pending", "errors": []},
        },
    }

    with patch("backend.import_status.get.get_step_function_validation_results") as validation_mock:
        validation_mock.return_value = []
        # When attempting to create the instance
        response = entrypoint.lambda_handler(
            {"httpMethod": "GET", "body": {"execution_arn": any_arn_formatted_string()}},
            any_lambda_context(),
        )

    # Then
    assert response == expected_response


@mark.infrastructure
def should_retrieve_validation_failures() -> None:
    # Given

    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()
    asset_id = f"DATASET#{dataset_id}#VERSION#{version_id}"

    url = any_s3_url()
    error_details = {"error_message": "test"}
    check = "example"

    expected_response = [
        {
            "check": check,
            "result": ValidationResult.FAILED.value,
            "url": url,
            "details": error_details,
        }
    ]

    with ValidationItem(
        asset_id=asset_id,
        result=ValidationResult.FAILED,
        details=error_details,
        url=url,
        check=check,
    ):

        # When
        response = get_step_function_validation_results(dataset_id, version_id)

        # Then
        assert response == expected_response


@patch("backend.import_status.get.STEP_FUNCTIONS_CLIENT.describe_execution")
@patch("backend.import_status.get.S3CONTROL_CLIENT.describe_job")
def should_report_s3_batch_upload_failures(
    describe_s3_job_mock: MagicMock,
    describe_step_function_mock: MagicMock,
) -> None:
    # Given
    describe_step_function_mock.return_value = {
        "status": "SUCCEEDED",
        "input": json.dumps(
            {"dataset_id": any_dataset_id(), "version_id": any_dataset_version_id()}
        ),
        "output": json.dumps({"s3_batch_copy": {"job_id": any_job_id()}}),
    }

    describe_s3_job_mock.return_value = {
        "Job": {
            "Status": "Completed",
            "FailureReasons": [{"FailureCode": "TEST_CODE", "FailureReason": "TEST_REASON"}],
        }
    }

    expected_response = {
        "statusCode": 200,
        "body": {
            "validation": {"status": "SUCCEEDED", "errors": []},
            "upload": {
                "status": "Completed",
                "errors": [{"FailureCode": "TEST_CODE", "FailureReason": "TEST_REASON"}],
            },
        },
    }
    with patch("backend.import_status.get.STS_CLIENT.get_caller_identity") as sts_mock, patch(
        "backend.import_status.get.get_step_function_validation_results"
    ) as validation_mock:
        validation_mock.return_value = []
        sts_mock.return_value = {"Account": "1234567890"}

        # When
        response = entrypoint.lambda_handler(
            {"httpMethod": "GET", "body": {"execution_arn": any_arn_formatted_string()}},
            any_lambda_context(),
        )

        # Then
        assert response == expected_response
