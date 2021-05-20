from json import dumps
from unittest.mock import MagicMock, patch
from urllib.parse import quote

from backend.import_asset_file.task import lambda_handler
from backend.import_dataset_file import (
    EXCEPTION_PREFIX,
    INVOCATION_ID_KEY,
    INVOCATION_SCHEMA_VERSION_KEY,
    RESULTS_KEY,
    RESULT_CODE_KEY,
    RESULT_CODE_PERMANENT_FAILURE,
    RESULT_STRING_KEY,
    S3_BUCKET_ARN_KEY,
    S3_KEY_KEY,
    TASKS_KEY,
    TASK_ID_KEY,
)
from backend.import_dataset_keys import NEW_KEY_KEY, ORIGINAL_KEY_KEY, TARGET_BUCKET_NAME_KEY

from .aws_utils import (
    any_invocation_id,
    any_invocation_schema_version,
    any_lambda_context,
    any_s3_bucket_arn,
    any_s3_bucket_name,
    any_task_id,
)
from .general_generators import any_error_message, any_safe_file_path


@patch("backend.import_asset_file.task.S3_CLIENT")
def should_treat_unhandled_exception_as_permanent_failure(s3_client_mock: MagicMock) -> None:
    # Given
    error_message = any_error_message()
    s3_client_mock.copy_object.side_effect = Exception(error_message)
    task_id = any_task_id()
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
        INVOCATION_ID_KEY: any_invocation_id(),
        INVOCATION_SCHEMA_VERSION_KEY: any_invocation_schema_version(),
    }

    response = lambda_handler(event, any_lambda_context())

    assert response[RESULTS_KEY] == [
        {
            TASK_ID_KEY: task_id,
            RESULT_CODE_KEY: RESULT_CODE_PERMANENT_FAILURE,
            RESULT_STRING_KEY: f"{EXCEPTION_PREFIX}: {error_message}",
        }
    ]
