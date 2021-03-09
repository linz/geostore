import logging
import sys
from copy import deepcopy
from json import dumps
from random import choice
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError  # type: ignore[import]

from backend.check_stac_metadata.task import STACDatasetValidator, main, parse_arguments

from .aws_utils import MINIMAL_VALID_STAC_OBJECT, MockJSONURLReader, any_s3_url
from .general_generators import any_program_name, any_safe_filename
from .stac_generators import (
    any_asset_name,
    any_dataset_id,
    any_dataset_version_id,
    any_hex_multihash,
)


class TestLogging:
    logger: logging.Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logging.getLogger("backend.check_stac_metadata.task")

    @patch("backend.check_stac_metadata.task.STACDatasetValidator.validate")
    def should_log_arguments(self, validate_url_mock: MagicMock) -> None:
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

        with patch.object(self.logger, "debug") as logger_mock:
            parse_arguments(self.logger)

            logger_mock.assert_any_call(expected_log)

    def should_log_on_validation_success(self) -> None:
        sys.argv = [
            any_program_name(),
            f"--metadata-url={any_s3_url()}",
            f"--dataset-id={any_dataset_id()}",
            f"--version-id={any_dataset_version_id()}",
        ]

        with patch.object(self.logger, "info") as logger_mock, patch(
            "backend.check_stac_metadata.task.STACDatasetValidator.validate"
        ):
            main()

            logger_mock.assert_any_call('{"success": true, "message": ""}')

    @patch("backend.check_stac_metadata.task.STACDatasetValidator.validate")
    def should_log_on_validation_failure(self, validate_url_mock: MagicMock) -> None:
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

    def should_log_assets(self) -> None:
        base_url = any_s3_url()
        metadata_url = f"{base_url}/{any_safe_filename()}"
        stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
        asset_url = f"{base_url}/{any_safe_filename()}"
        asset_multihash = any_hex_multihash()
        stac_object["item_assets"] = {
            any_asset_name(): {
                "href": asset_url,
                "checksum:multihash": asset_multihash,
            },
        }

        url_reader = MockJSONURLReader({metadata_url: stac_object})
        expected_message = dumps({"asset": {"url": asset_url, "multihash": asset_multihash}})

        with patch.object(self.logger, "debug") as logger_mock:
            STACDatasetValidator(url_reader, self.logger).validate(metadata_url)

            logger_mock.assert_any_call(expected_message)
