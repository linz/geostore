import logging
import sys
from json import dumps
from os import environ
from unittest.mock import patch

from pytest import mark
from pytest_subtests import SubTests  # type: ignore[import]

from backend.check_files_checksums.task import main
from backend.check_files_checksums.utils import ARRAY_INDEX_VARIABLE_NAME
from backend.processing_assets_model import ProcessingAssetType, ProcessingAssetsModel

from .general_generators import any_program_name
from .stac_generators import any_dataset_id, any_dataset_version_id


class TestLogging:
    logger: logging.Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logging.getLogger("backend.check_files_checksums.task")

    @mark.infrastructure
    def should_log_missing_item(self, subtests: SubTests) -> None:
        # Given
        dataset_id = any_dataset_id()
        version_id = any_dataset_version_id()
        index = 0
        expected_log = dumps(
            {
                "success": False,
                "error": {"message": ProcessingAssetsModel.DoesNotExist.msg},
                "parameters": {
                    "hash_key": f"DATASET#{dataset_id}#VERSION#{version_id}",
                    "range_key": f"{ProcessingAssetType.DATA.value}#{index}",
                },
            }
        )

        environ[ARRAY_INDEX_VARIABLE_NAME] = "0"
        sys.argv = [
            any_program_name(),
            f"--dataset-id={dataset_id}",
            f"--version-id={version_id}",
            f"--first-item={index}",
        ]

        # When/Then
        with patch.object(self.logger, "error") as logger_mock:
            with subtests.test(msg="Return code"):
                assert main() == 1

            with subtests.test(msg="Log message"):
                logger_mock.assert_any_call(expected_log)
