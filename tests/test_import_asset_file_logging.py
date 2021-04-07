import logging
from json import dumps
from unittest.mock import patch
from urllib.parse import quote

from backend.import_asset_file.task import lambda_handler
from backend.keys.import_dataset_keys import NEW_KEY_KEY, ORIGINAL_KEY_KEY

from .aws_utils import any_lambda_context, any_s3_bucket_arn
from .general_generators import any_safe_file_path

LOGGER = logging.getLogger("backend.import_asset_file.task")


def should_log_payload() -> None:
    # Given
    event = {
        "tasks": [
            {
                "s3BucketArn": any_s3_bucket_arn(),
                "s3Key": quote(
                    dumps(
                        {
                            ORIGINAL_KEY_KEY: any_safe_file_path(),
                            NEW_KEY_KEY: any_safe_file_path(),
                        }
                    )
                ),
                "taskId": "any task ID",
            }
        ],
        "invocationId": "any invocation ID",
        "invocationSchemaVersion": "any invocation schema version",
    }

    with patch.object(LOGGER, "debug") as logger_mock:
        # When
        lambda_handler(event, any_lambda_context())

        # Then
        logger_mock.assert_any_call(dumps(event))
