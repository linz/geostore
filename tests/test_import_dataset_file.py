import logging
from json import dumps
from unittest.mock import MagicMock, patch
from urllib.parse import quote

from backend.import_dataset_file import (
    INVOCATION_ID_KEY,
    INVOCATION_SCHEMA_VERSION_KEY,
    S3_BUCKET_ARN_KEY,
    S3_KEY_KEY,
    TASKS_KEY,
    TASK_ID_KEY,
    get_import_result,
)
from backend.import_dataset_keys import NEW_KEY_KEY, ORIGINAL_KEY_KEY, TARGET_BUCKET_NAME_KEY

from .aws_utils import any_s3_bucket_arn, any_s3_bucket_name
from .general_generators import any_safe_file_path

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
