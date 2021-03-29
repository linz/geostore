import logging
import sys
from copy import deepcopy
from json import dumps
from unittest.mock import MagicMock, patch

from backend.check_stac_metadata.utils import STACDatasetValidator, parse_arguments

from .aws_utils import (
    MINIMAL_VALID_STAC_OBJECT,
    MockJSONURLReader,
    MockValidationResultFactory,
    any_s3_url,
)
from .general_generators import any_program_name, any_safe_filename
from .stac_generators import (
    any_asset_name,
    any_dataset_id,
    any_dataset_version_id,
    any_hex_multihash,
)

LOGGER = logging.getLogger("backend.check_stac_metadata.utils")


@patch("backend.check_stac_metadata.task.STACDatasetValidator.validate")
def should_log_arguments(validate_url_mock: MagicMock) -> None:
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

    with patch.object(LOGGER, "debug") as logger_mock:
        parse_arguments(LOGGER)

        logger_mock.assert_any_call(expected_log)


def should_log_assets() -> None:
    base_url = any_s3_url()
    metadata_url = f"{base_url}/{any_safe_filename()}"
    stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
    asset_url = f"{base_url}/{any_safe_filename()}"
    asset_multihash = any_hex_multihash()
    stac_object["assets"] = {
        any_asset_name(): {
            "href": asset_url,
            "checksum:multihash": asset_multihash,
        },
    }

    url_reader = MockJSONURLReader({metadata_url: stac_object})
    expected_message = dumps({"asset": {"url": asset_url, "multihash": asset_multihash}})

    with patch.object(LOGGER, "debug") as logger_mock, patch(
        "backend.check_stac_metadata.utils.processing_assets_model_with_meta"
    ):
        STACDatasetValidator(url_reader, MockValidationResultFactory()).validate(metadata_url)

        logger_mock.assert_any_call(expected_message)
