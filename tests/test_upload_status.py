from typing import cast
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError
from pytest import raises

from backend.api_keys import SUCCESS_KEY
from backend.import_file_batch_job_id_keys import ASSET_JOB_ID_KEY, METADATA_JOB_ID_KEY
from backend.step_function import Outcome
from backend.step_function_keys import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_KEY,
    ERRORS_KEY,
    FAILED_TASKS_KEY,
    FAILURE_REASONS_KEY,
    IMPORT_DATASET_KEY,
    METADATA_UPLOAD_KEY,
    STATUS_KEY,
    VALIDATION_KEY,
    VERSION_ID_KEY,
)
from backend.types import JsonObject
from backend.upload_status.task import lambda_handler

from .aws_utils import any_account_id, any_batch_job_status, any_job_id, any_lambda_context
from .stac_generators import any_dataset_id, any_dataset_version_id


def should_raise_exception_when_missing_mandatory_execution_arn() -> None:
    with raises(ValidationError):
        lambda_handler({}, any_lambda_context())


@patch("backend.step_function.get_step_function_validation_results")
@patch("backend.step_function.S3CONTROL_CLIENT.describe_job")
@patch("backend.step_function.get_account_number")
def should_report_upload_statuses(
    get_account_number_mock: MagicMock,
    describe_job_mock: MagicMock,
    get_step_function_validation_results_mock: MagicMock,
) -> None:
    # Given
    account_id = any_account_id()
    get_account_number_mock.return_value = account_id
    asset_job_id = any_job_id()
    asset_job_status = any_batch_job_status()
    metadata_job_id = any_job_id()
    metadata_job_status = any_batch_job_status()

    get_step_function_validation_results_mock.return_value = []

    def describe_job(AccountId: str, JobId: str) -> JsonObject:  # pylint: disable=invalid-name
        assert AccountId == cast(str, account_id)
        return {
            asset_job_id: {
                "Job": {
                    "Status": asset_job_status,
                    "FailureReasons": [],
                    "ProgressSummary": {"NumberOfTasksFailed": 0},
                }
            },
            metadata_job_id: {
                "Job": {
                    "Status": metadata_job_status,
                    "FailureReasons": [],
                    "ProgressSummary": {"NumberOfTasksFailed": 0},
                }
            },
        }[JobId]

    describe_job_mock.side_effect = describe_job

    expected_response = {
        VALIDATION_KEY: {STATUS_KEY: Outcome.PASSED.value, ERRORS_KEY: []},
        ASSET_UPLOAD_KEY: {
            STATUS_KEY: asset_job_status,
            ERRORS_KEY: {FAILED_TASKS_KEY: 0, FAILURE_REASONS_KEY: []},
        },
        METADATA_UPLOAD_KEY: {
            STATUS_KEY: metadata_job_status,
            ERRORS_KEY: {FAILED_TASKS_KEY: 0, FAILURE_REASONS_KEY: []},
        },
    }

    # When
    response = lambda_handler(
        {
            DATASET_ID_KEY: any_dataset_id(),
            VERSION_ID_KEY: any_dataset_version_id(),
            VALIDATION_KEY: {SUCCESS_KEY: True},
            IMPORT_DATASET_KEY: {
                METADATA_JOB_ID_KEY: metadata_job_id,
                ASSET_JOB_ID_KEY: asset_job_id,
            },
        },
        any_lambda_context(),
    )

    # Then
    assert response == expected_response
