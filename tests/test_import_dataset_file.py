from json import dumps
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, call, patch
from urllib.parse import quote

from botocore.exceptions import ClientError

from geostore.aws_response import AWS_CODE_REQUEST_TIMEOUT
from geostore.import_dataset_file import (
    INVOCATION_ID_KEY,
    INVOCATION_SCHEMA_VERSION_KEY,
    LOG_MESSAGE_S3_BATCH_COPY_RESULT,
    RESULTS_KEY,
    RESULT_CODE_KEY,
    RESULT_CODE_PERMANENT_FAILURE,
    RESULT_CODE_SUCCEEDED,
    RESULT_CODE_TEMPORARY_FAILURE,
    RESULT_STRING_KEY,
    RETRY_RESULT_STRING,
    S3_BUCKET_ARN_KEY,
    S3_KEY_KEY,
    TASKS_KEY,
    TASK_ID_KEY,
    TREAT_MISSING_KEYS_AS_KEY,
    get_import_result,
)
from geostore.import_dataset_keys import NEW_KEY_KEY, ORIGINAL_KEY_KEY, TARGET_BUCKET_NAME_KEY
from geostore.logging_keys import LOG_MESSAGE_LAMBDA_START
from geostore.step_function_keys import S3_ROLE_ARN_KEY
from geostore.types import JsonObject

from .aws_utils import (
    any_invocation_id,
    any_invocation_schema_version,
    any_operation_name,
    any_role_arn,
    any_s3_bucket_arn,
    any_s3_bucket_name,
    any_task_id,
)
from .general_generators import any_error_message, any_safe_file_path

if TYPE_CHECKING:
    from botocore.exceptions import (  # pylint:disable=no-name-in-module,ungrouped-imports
        ClientErrorResponseError,
        ClientErrorResponseTypeDef,
    )
else:
    ClientErrorResponseError = ClientErrorResponseTypeDef = dict


@patch("geostore.import_metadata_file.task.importer")
def should_log_payload(importer_mock: MagicMock) -> None:
    # Given
    event = {
        TASKS_KEY: [
            {
                S3_BUCKET_ARN_KEY: any_s3_bucket_arn(),
                S3_KEY_KEY: quote(
                    dumps(
                        {
                            NEW_KEY_KEY: any_safe_file_path(),
                            ORIGINAL_KEY_KEY: any_safe_file_path(),
                            S3_ROLE_ARN_KEY: any_role_arn(),
                            TARGET_BUCKET_NAME_KEY: any_s3_bucket_name(),
                        }
                    )
                ),
                TASK_ID_KEY: any_task_id(),
            }
        ],
        INVOCATION_ID_KEY: any_invocation_id(),
        INVOCATION_SCHEMA_VERSION_KEY: any_invocation_schema_version(),
    }
    expected_log_call = call(LOG_MESSAGE_LAMBDA_START, lambda_input=event)

    with patch("geostore.import_dataset_file.LOGGER.debug") as logger_mock, patch(
        "geostore.import_dataset_file.get_s3_client_for_role"
    ):
        # When
        get_import_result(event, importer_mock)

        # Then
        logger_mock.assert_has_calls([expected_log_call])


@patch("geostore.import_metadata_file.task.importer")
def should_log_result(importer_mock: MagicMock) -> None:
    # Given
    importer_response: JsonObject = {}
    importer_mock.return_value = importer_response
    invocation_schema_version = any_invocation_schema_version()
    invocation_id = any_invocation_id()
    task_id = any_task_id()
    event = {
        TASKS_KEY: [
            {
                S3_BUCKET_ARN_KEY: any_s3_bucket_arn(),
                S3_KEY_KEY: quote(
                    dumps(
                        {
                            NEW_KEY_KEY: any_safe_file_path(),
                            ORIGINAL_KEY_KEY: any_safe_file_path(),
                            S3_ROLE_ARN_KEY: any_role_arn(),
                            TARGET_BUCKET_NAME_KEY: any_s3_bucket_name(),
                        }
                    )
                ),
                TASK_ID_KEY: task_id,
            }
        ],
        INVOCATION_ID_KEY: invocation_id,
        INVOCATION_SCHEMA_VERSION_KEY: invocation_schema_version,
    }
    expected_log_entry = {
        INVOCATION_SCHEMA_VERSION_KEY: invocation_schema_version,
        TREAT_MISSING_KEYS_AS_KEY: RESULT_CODE_PERMANENT_FAILURE,
        INVOCATION_ID_KEY: invocation_id,
        RESULTS_KEY: [
            {
                TASK_ID_KEY: task_id,
                RESULT_CODE_KEY: RESULT_CODE_SUCCEEDED,
                RESULT_STRING_KEY: str(importer_response),
            }
        ],
    }

    expected_log_call = call(LOG_MESSAGE_S3_BATCH_COPY_RESULT, result=expected_log_entry)

    with patch("geostore.import_dataset_file.LOGGER.debug") as logger_mock, patch(
        "geostore.import_dataset_file.get_s3_client_for_role"
    ):
        get_import_result(event, importer_mock)

        # Then
        logger_mock.assert_has_calls([expected_log_call])


@patch("geostore.import_metadata_file.task.importer")
def should_treat_timeout_as_a_temporary_failure(importer_mock: MagicMock) -> None:
    # Given
    task_id = any_task_id()
    invocation_id = any_invocation_id()
    invocation_schema_version = any_invocation_schema_version()

    importer_mock.side_effect = ClientError(
        ClientErrorResponseTypeDef(Error=ClientErrorResponseError(Code=AWS_CODE_REQUEST_TIMEOUT)),
        any_operation_name(),
    )

    event = {
        TASKS_KEY: [
            {
                S3_BUCKET_ARN_KEY: any_s3_bucket_arn(),
                S3_KEY_KEY: quote(
                    dumps(
                        {
                            NEW_KEY_KEY: any_safe_file_path(),
                            ORIGINAL_KEY_KEY: any_safe_file_path(),
                            S3_ROLE_ARN_KEY: any_role_arn(),
                            TARGET_BUCKET_NAME_KEY: any_s3_bucket_name(),
                        }
                    )
                ),
                TASK_ID_KEY: task_id,
            }
        ],
        INVOCATION_ID_KEY: invocation_id,
        INVOCATION_SCHEMA_VERSION_KEY: invocation_schema_version,
    }

    # When
    with patch("geostore.import_dataset_file.get_s3_client_for_role"):
        response = get_import_result(event, importer_mock)

    # Then
    assert response == {
        INVOCATION_SCHEMA_VERSION_KEY: invocation_schema_version,
        TREAT_MISSING_KEYS_AS_KEY: RESULT_CODE_PERMANENT_FAILURE,
        INVOCATION_ID_KEY: invocation_id,
        RESULTS_KEY: [
            {
                TASK_ID_KEY: task_id,
                RESULT_CODE_KEY: RESULT_CODE_TEMPORARY_FAILURE,
                RESULT_STRING_KEY: RETRY_RESULT_STRING,
            }
        ],
    }


@patch("geostore.import_metadata_file.task.importer")
def should_treat_unknown_error_code_as_permanent_failure(importer_mock: MagicMock) -> None:
    # Given
    task_id = any_task_id()
    invocation_id = any_invocation_id()
    invocation_schema_version = any_invocation_schema_version()

    error_code = f"not {AWS_CODE_REQUEST_TIMEOUT}"
    error_message = any_error_message()
    operation_name = any_operation_name()

    importer_mock.side_effect = ClientError(
        ClientErrorResponseTypeDef(
            Error=ClientErrorResponseError(Code=error_code, Message=error_message)
        ),
        operation_name,
    )

    event = {
        TASKS_KEY: [
            {
                S3_BUCKET_ARN_KEY: any_s3_bucket_arn(),
                S3_KEY_KEY: quote(
                    dumps(
                        {
                            NEW_KEY_KEY: any_safe_file_path(),
                            ORIGINAL_KEY_KEY: any_safe_file_path(),
                            S3_ROLE_ARN_KEY: any_role_arn(),
                            TARGET_BUCKET_NAME_KEY: any_s3_bucket_name(),
                        }
                    )
                ),
                TASK_ID_KEY: task_id,
            }
        ],
        INVOCATION_ID_KEY: invocation_id,
        INVOCATION_SCHEMA_VERSION_KEY: invocation_schema_version,
    }

    # When
    with patch("geostore.import_dataset_file.get_s3_client_for_role"):
        response = get_import_result(event, importer_mock)

    # Then
    assert response == {
        INVOCATION_SCHEMA_VERSION_KEY: invocation_schema_version,
        TREAT_MISSING_KEYS_AS_KEY: RESULT_CODE_PERMANENT_FAILURE,
        INVOCATION_ID_KEY: invocation_id,
        RESULTS_KEY: [
            {
                TASK_ID_KEY: task_id,
                RESULT_CODE_KEY: RESULT_CODE_PERMANENT_FAILURE,
                RESULT_STRING_KEY: f"{error_code} when calling {operation_name}: {error_message}",
            }
        ],
    }
