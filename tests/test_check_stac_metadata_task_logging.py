import logging
import sys
from random import choice
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError  # type: ignore[import]

from backend.check_stac_metadata.task import main

from .aws_utils import any_s3_url
from .general_generators import any_program_name
from .stac_generators import any_dataset_id, any_dataset_version_id

LOGGER = logging.getLogger("backend.check_stac_metadata.task")


def should_log_on_validation_success() -> None:
    sys.argv = [
        any_program_name(),
        f"--metadata-url={any_s3_url()}",
        f"--dataset-id={any_dataset_id()}",
        f"--version-id={any_dataset_version_id()}",
    ]

    with patch.object(LOGGER, "info") as logger_mock, patch(
        "backend.check_stac_metadata.task.STACDatasetValidator.validate"
    ):
        main()

        logger_mock.assert_any_call('{"success": true, "message": ""}')


@patch("backend.check_stac_metadata.task.STACDatasetValidator.validate")
def should_log_on_validation_failure(validate_url_mock: MagicMock) -> None:
    error_message = "Some error message"
    validate_url_mock.side_effect = choice([ValidationError, AssertionError])(error_message)
    sys.argv = [
        any_program_name(),
        f"--metadata-url={any_s3_url()}",
        f"--dataset-id={any_dataset_id()}",
        f"--version-id={any_dataset_version_id()}",
    ]

    with patch.object(LOGGER, "error") as logger_mock:
        main()

        logger_mock.assert_any_call(f'{{"success": false, "message": "{error_message}"}}')
