import logging
import sys
from json import dumps
from random import choice
from unittest import TestCase
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError  # type: ignore[import]

from ..processing.check_stac_metadata.task import main
from .utils import any_dataset_id, any_dataset_version_id, any_program_name, any_s3_url


class LogTests(TestCase):
    logger: logging.Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logging.getLogger("datalake.backend.processing.check_stac_metadata.task")

    @patch("datalake.backend.processing.check_stac_metadata.task.STACSchemaValidator.validate")
    def test_should_log_arguments(self, validate_url_mock: MagicMock) -> None:
        validate_url_mock.return_value = set()
        url = any_s3_url()
        dataset_id = any_dataset_id()
        version_id = any_dataset_version_id()
        expected_log = dumps(
            {"arguments": {"metadata_url": url, "dataset_id": dataset_id, "version_id": version_id}}
        )

        sys.argv = [
            any_program_name(),
            f"--metadata-url={url}",
            f"--dataset-id={dataset_id}",
            f"--version-id={version_id}",
        ]

        with patch.object(self.logger, "debug") as logger_mock, patch(
            "datalake.backend.processing.check_stac_metadata.task.ProcessingAssetsModel"
        ):
            main()

            logger_mock.assert_any_call(expected_log)

    def test_should_print_json_output_on_validation_success(self) -> None:
        sys.argv = [
            any_program_name(),
            f"--metadata-url={any_s3_url()}",
            f"--dataset-id={any_dataset_id()}",
            f"--version-id={any_dataset_version_id()}",
        ]

        with patch.object(self.logger, "info") as logger_mock, patch(
            "datalake.backend.processing.check_stac_metadata.task.STACSchemaValidator.validate"
        ):
            main()

            logger_mock.assert_any_call('{"success": true, "message": ""}')

    @patch("datalake.backend.processing.check_stac_metadata.task.STACSchemaValidator.validate")
    def test_should_print_json_output_on_validation_failure(
        self, validate_url_mock: MagicMock
    ) -> None:
        error_message = "Some error message"
        validate_url_mock.side_effect = choice([ValidationError, AssertionError])(error_message)
        sys.argv = [
            any_program_name(),
            f"--metadata-url={any_s3_url()}",
            f"--dataset-id={any_dataset_id()}",
            f"--version-id={any_dataset_version_id()}",
        ]

        with patch.object(self.logger, "error") as logger_mock:
            main()

            logger_mock.assert_any_call(f'{{"success": false, "message": "{error_message}"}}')
