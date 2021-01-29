import logging
import sys
from argparse import Namespace
from random import choice
from unittest import TestCase
from unittest.mock import patch

from jsonschema import ValidationError  # type: ignore[import]

from ..processing.check_stac_metadata.task import main
from .utils import any_program_name, any_s3_url


class LogTests(TestCase):
    logger: logging.Logger

    @classmethod
    def setup_class(cls):
        cls.logger = logging.getLogger("datalake.backend.processing.check_stac_metadata.task")

    @patch("datalake.backend.processing.check_stac_metadata.task.STACSchemaValidator.validate")
    def test_should_log_arguments(self, validate_url_mock) -> None:
        validate_url_mock.return_value = None
        url = any_s3_url()
        sys.argv = [any_program_name(), f"--metadata-url={url}"]

        with patch.object(self.logger, "debug") as logger_mock:
            main()

            logger_mock.assert_called_once_with(Namespace(metadata_url=url))

    def test_should_print_json_output_on_validation_success(self) -> None:
        sys.argv = [any_program_name(), f"--metadata-url={any_s3_url()}"]

        with patch.object(self.logger, "info") as logger_mock, patch(
            "datalake.backend.processing.check_stac_metadata.task.STACSchemaValidator.validate"
        ):
            main()

            logger_mock.assert_called_once_with('{"success": true, "message": ""}')

    @patch("datalake.backend.processing.check_stac_metadata.task.STACSchemaValidator.validate")
    def test_should_print_json_output_on_validation_failure(self, validate_url_mock) -> None:
        error_message = "Some error message"
        validate_url_mock.side_effect = choice([ValidationError, AssertionError])(error_message)
        sys.argv = [any_program_name(), f"--metadata-url={any_s3_url()}"]

        with patch.object(self.logger, "error") as logger_mock:
            main()

            logger_mock.assert_called_once_with(
                f'{{"success": false, "message": "{error_message}"}}'
            )
