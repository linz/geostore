import logging
from json import dumps
from unittest.mock import patch
from urllib.parse import quote

from backend.import_asset_file.task import lambda_handler
from backend.import_dataset_keys import (
    INVOCATION_ID_KEY,
    INVOCATION_SCHEMA_VERSION_KEY,
    NEW_KEY_KEY,
    ORIGINAL_KEY_KEY,
    S3_BUCKET_ARN_KEY,
    S3_KEY_KEY,
    TARGET_BUCKET_NAME_KEY,
    TASKS_KEY,
    TASK_ID_KEY,
)

from .aws_utils import any_lambda_context, any_s3_bucket_arn, any_s3_bucket_name
from .general_generators import any_safe_file_path

LOGGER = logging.getLogger("backend.import_asset_file.task")


def should_log_payload() -> None:
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
        lambda_handler(event, any_lambda_context())

        # Then
        logger_mock.assert_any_call(dumps(event))
