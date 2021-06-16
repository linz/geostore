import logging
from json import dumps
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch
from urllib.parse import quote

from botocore.exceptions import ClientError

from backend.aws_response import AWS_CODE_REQUEST_TIMEOUT
from backend.import_dataset_file import (
    INVOCATION_ID_KEY,
    INVOCATION_SCHEMA_VERSION_KEY,
    RESULTS_KEY,
    RESULT_CODE_KEY,
    RESULT_CODE_PERMANENT_FAILURE,
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
from backend.import_dataset_keys import NEW_KEY_KEY, ORIGINAL_KEY_KEY, TARGET_BUCKET_NAME_KEY

from .aws_utils import (
    any_invocation_id,
    any_invocation_schema_version,
    any_operation_name,
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

LOGGER = logging.getLogger("backend.import_dataset_file")


@patch("backend.import_metadata_file.task.importer")
def should_log_payload(importer_mock: MagicMock) -> None:
    # Given
    event = {
        TASKS_KEY: [
            {
                S3_BUCKET_ARN_KEY: any_s3_bucket_arn(),
                S3_KEY_KEY: quote(
                    dumps(
                        {
                            TARGET_BUCKET_NAME_KEY: any_s3_bucket_name(),
                            ORIGINAL_KEY_KEY: any_safe_file_path(),
                            NEW_KEY_KEY: any_safe_file_path(),
                        }
                    )
                ),
                TASK_ID_KEY: "any task ID",
            }
        ],
        INVOCATION_ID_KEY: "any invocation ID",
        INVOCATION_SCHEMA_VERSION_KEY: "any invocation schema version",
    }

    with patch.object(LOGGER, "debug") as logger_mock:
        # When
        get_import_result(event, importer_mock)

        # Then
        logger_mock.assert_any_call(dumps(event))


@patch("backend.import_metadata_file.task.importer")
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
                            TARGET_BUCKET_NAME_KEY: any_s3_bucket_name(),
                            ORIGINAL_KEY_KEY: any_safe_file_path(),
                            NEW_KEY_KEY: any_safe_file_path(),
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


@patch("backend.import_metadata_file.task.importer")
def should_treat_unknown_error_code_as_permanent_failure(importer_mock: MagicMock) -> None:
    # Given
    task_id = any_task_id()
    invocation_id = any_invocation_id()
    invocation_schema_version = any_invocation_schema_version()

    error_code = f"not {AWS_CODE_REQUEST_TIMEOUT}"
    error_message = any_error_message()

    importer_mock.side_effect = ClientError(
        ClientErrorResponseTypeDef(
            Error=ClientErrorResponseError(Code=error_code, Message=error_message)
        ),
        any_operation_name(),
    )

    event = {
        TASKS_KEY: [
            {
                S3_BUCKET_ARN_KEY: any_s3_bucket_arn(),
                S3_KEY_KEY: quote(
                    dumps(
                        {
                            TARGET_BUCKET_NAME_KEY: any_s3_bucket_name(),
                            ORIGINAL_KEY_KEY: any_safe_file_path(),
                            NEW_KEY_KEY: any_safe_file_path(),
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
                RESULT_STRING_KEY: f"{error_code}: {error_message}",
            }
        ],
    }
