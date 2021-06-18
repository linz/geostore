"""
Dataset Versions endpoint Lambda function tests.
"""
from http import HTTPStatus
from json import dumps
from unittest.mock import MagicMock, patch

from pytest import mark

from backend.api_keys import MESSAGE_KEY, STATUS_KEY, SUCCESS_KEY
from backend.api_responses import BODY_KEY, HTTP_METHOD_KEY, STATUS_CODE_KEY
from backend.import_file_batch_job_id_keys import ASSET_JOB_ID_KEY, METADATA_JOB_ID_KEY
from backend.import_status import entrypoint
from backend.models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR, VERSION_ID_PREFIX
from backend.step_function import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_KEY,
    ERRORS_KEY,
    ERROR_CHECK_KEY,
    ERROR_DETAILS_KEY,
    ERROR_RESULT_KEY,
    ERROR_URL_KEY,
    EXECUTION_ARN_KEY,
    IMPORT_DATASET_KEY,
    METADATA_UPLOAD_KEY,
    STEP_FUNCTION_KEY,
    VALIDATION_KEY,
    VERSION_ID_KEY,
    Outcome,
)
from backend.validation_results_model import ValidationResult

from .aws_utils import (
    ValidationItem,
    any_account_id,
    any_arn_formatted_string,
    any_job_id,
    any_lambda_context,
    any_s3_url,
)
from .stac_generators import any_dataset_id, any_dataset_version_id


def should_return_required_property_error_when_missing_mandatory_execution_arn() -> None:
    # Given an empty body
    response = entrypoint.lambda_handler(
        {HTTP_METHOD_KEY: "GET", BODY_KEY: {}}, any_lambda_context()
    )

    # Then the API should return an error message
    assert response == {
        STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
        BODY_KEY: {MESSAGE_KEY: f"Bad Request: '{EXECUTION_ARN_KEY}' is a required property"},
    }


@patch("backend.import_status.get.STEP_FUNCTIONS_CLIENT.describe_execution")
def should_report_upload_status_as_pending_when_validation_incomplete(
    describe_execution_mock: MagicMock,
) -> None:
    # Given
    describe_execution_mock.return_value = {
        "status": "RUNNING",
        "input": dumps(
            {DATASET_ID_KEY: any_dataset_id(), VERSION_ID_KEY: any_dataset_version_id()}
        ),
    }

    expected_response = {
        STATUS_CODE_KEY: HTTPStatus.OK,
        BODY_KEY: {
            STEP_FUNCTION_KEY: {STATUS_KEY: "Running"},
            VALIDATION_KEY: {STATUS_KEY: Outcome.PENDING.value, ERRORS_KEY: []},
            METADATA_UPLOAD_KEY: {STATUS_KEY: Outcome.PENDING.value, ERRORS_KEY: []},
            ASSET_UPLOAD_KEY: {STATUS_KEY: Outcome.PENDING.value, ERRORS_KEY: []},
        },
    }

    with patch("backend.step_function.get_step_function_validation_results") as validation_mock:
        validation_mock.return_value = []
        # When attempting to create the instance
        response = entrypoint.lambda_handler(
            {HTTP_METHOD_KEY: "GET", BODY_KEY: {EXECUTION_ARN_KEY: any_arn_formatted_string()}},
            any_lambda_context(),
        )

    # Then
    assert response == expected_response


@mark.infrastructure
@patch("backend.import_status.get.STEP_FUNCTIONS_CLIENT.describe_execution")
def should_retrieve_validation_failures(describe_step_function_mock: MagicMock) -> None:
    # Given

    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    describe_step_function_mock.return_value = {
        "status": "SUCCEEDED",
        "input": dumps({DATASET_ID_KEY: dataset_id, VERSION_ID_KEY: version_id}),
        "output": dumps({VALIDATION_KEY: {SUCCESS_KEY: False}}),
    }

    url = any_s3_url()
    error_details = {"error_message": "test"}
    check = "example"

    expected_response = {
        STATUS_CODE_KEY: HTTPStatus.OK,
        BODY_KEY: {
            STEP_FUNCTION_KEY: {STATUS_KEY: "Succeeded"},
            VALIDATION_KEY: {
                STATUS_KEY: Outcome.FAILED.value,
                ERRORS_KEY: [
                    {
                        ERROR_CHECK_KEY: check,
                        ERROR_DETAILS_KEY: error_details,
                        ERROR_RESULT_KEY: ValidationResult.FAILED.value,
                        ERROR_URL_KEY: url,
                    }
                ],
            },
            METADATA_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
            ASSET_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
        },
    }
    with ValidationItem(
        asset_id=(
            f"{DATASET_ID_PREFIX}{dataset_id}{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{version_id}"
        ),
        result=ValidationResult.FAILED,
        details=error_details,
        url=url,
        check=check,
    ):
        # When
        response = entrypoint.lambda_handler(
            {HTTP_METHOD_KEY: "GET", BODY_KEY: {EXECUTION_ARN_KEY: any_arn_formatted_string()}},
            any_lambda_context(),
        )

        # Then
        assert response == expected_response


@patch("backend.import_status.get.STEP_FUNCTIONS_CLIENT.describe_execution")
@patch("backend.step_function.S3CONTROL_CLIENT.describe_job")
def should_report_s3_batch_upload_failures(
    describe_s3_job_mock: MagicMock,
    describe_step_function_mock: MagicMock,
) -> None:
    # Given
    describe_step_function_mock.return_value = {
        "status": "SUCCEEDED",
        "input": dumps(
            {DATASET_ID_KEY: any_dataset_id(), VERSION_ID_KEY: any_dataset_version_id()}
        ),
        "output": dumps(
            {
                VALIDATION_KEY: {SUCCESS_KEY: True},
                IMPORT_DATASET_KEY: {
                    METADATA_JOB_ID_KEY: any_job_id(),
                    ASSET_JOB_ID_KEY: any_job_id(),
                },
            }
        ),
    }

    describe_s3_job_mock.return_value = {
        "Job": {
            "Status": "Completed",
            "FailureReasons": [{"FailureCode": "TEST_CODE", "FailureReason": "TEST_REASON"}],
        }
    }

    expected_response = {
        STATUS_CODE_KEY: HTTPStatus.OK,
        BODY_KEY: {
            STEP_FUNCTION_KEY: {STATUS_KEY: "Succeeded"},
            VALIDATION_KEY: {STATUS_KEY: Outcome.PASSED.value, ERRORS_KEY: []},
            METADATA_UPLOAD_KEY: {
                STATUS_KEY: "Completed",
                ERRORS_KEY: [{"FailureCode": "TEST_CODE", "FailureReason": "TEST_REASON"}],
            },
            ASSET_UPLOAD_KEY: {
                STATUS_KEY: "Completed",
                ERRORS_KEY: [{"FailureCode": "TEST_CODE", "FailureReason": "TEST_REASON"}],
            },
        },
    }
    with patch("backend.step_function.get_account_number") as get_account_number_mock, patch(
        "backend.step_function.get_step_function_validation_results"
    ) as validation_mock:
        validation_mock.return_value = []
        get_account_number_mock.return_value = any_account_id()

        # When
        response = entrypoint.lambda_handler(
            {HTTP_METHOD_KEY: "GET", BODY_KEY: {EXECUTION_ARN_KEY: any_arn_formatted_string()}},
            any_lambda_context(),
        )

        # Then
        assert response == expected_response


@patch("backend.step_function.get_step_function_validation_results")
@patch("backend.import_status.get.STEP_FUNCTIONS_CLIENT.describe_execution")
@patch("backend.step_function.get_account_number")
def should_report_validation_as_skipped_if_not_started_due_to_failing_pipeline(
    get_account_number_mock: MagicMock,
    describe_step_function_mock: MagicMock,
    get_step_function_validation_results_mock: MagicMock,
) -> None:
    get_account_number_mock.return_value = any_account_id()
    describe_step_function_mock.return_value = {
        "status": "FAILED",
        "input": dumps(
            {DATASET_ID_KEY: any_dataset_id(), VERSION_ID_KEY: any_dataset_version_id()}
        ),
        "output": dumps({}),
    }
    get_step_function_validation_results_mock.return_value = []

    expected_response = {
        STATUS_CODE_KEY: HTTPStatus.OK,
        BODY_KEY: {
            STEP_FUNCTION_KEY: {STATUS_KEY: "Failed"},
            VALIDATION_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
            METADATA_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
            ASSET_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
        },
    }

    # When attempting to create the instance
    response = entrypoint.lambda_handler(
        {HTTP_METHOD_KEY: "GET", BODY_KEY: {EXECUTION_ARN_KEY: any_arn_formatted_string()}},
        any_lambda_context(),
    )

    # Then
    assert response == expected_response


@patch("backend.step_function.get_step_function_validation_results")
@patch("backend.import_status.get.STEP_FUNCTIONS_CLIENT.describe_execution")
@patch("backend.step_function.get_account_number")
def should_fail_validation_if_it_has_errors_but_step_function_does_not_report_status(
    get_account_number_mock: MagicMock,
    describe_step_function_mock: MagicMock,
    get_step_function_validation_results_mock: MagicMock,
) -> None:
    # Given
    get_account_number_mock.return_value = any_account_id()
    describe_step_function_mock.return_value = {
        "status": "FAILED",
        "input": dumps(
            {DATASET_ID_KEY: any_dataset_id(), VERSION_ID_KEY: any_dataset_version_id()}
        ),
        "output": dumps({}),
    }
    validation_error = {ERROR_RESULT_KEY: ValidationResult.FAILED.value}
    get_step_function_validation_results_mock.return_value = [validation_error]
    expected_response = {
        STATUS_CODE_KEY: HTTPStatus.OK,
        BODY_KEY: {
            STEP_FUNCTION_KEY: {STATUS_KEY: "Failed"},
            VALIDATION_KEY: {STATUS_KEY: Outcome.FAILED.value, ERRORS_KEY: [validation_error]},
            METADATA_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
            ASSET_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
        },
    }

    # When
    response = entrypoint.lambda_handler(
        {HTTP_METHOD_KEY: "GET", BODY_KEY: {EXECUTION_ARN_KEY: any_arn_formatted_string()}},
        any_lambda_context(),
    )

    # Then
    assert response == expected_response
