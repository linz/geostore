import logging
from copy import deepcopy
from io import StringIO
from json import JSONDecodeError, dumps
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError  # type: ignore[import]
from jsonschema import ValidationError  # type: ignore[import]

from backend.check_stac_metadata.utils import S3_URL_PREFIX, STACDatasetValidator

from .aws_utils import MockJSONURLReader, MockValidationResultFactory, any_s3_url
from .general_generators import any_error_message, any_https_url, any_safe_filename
from .stac_generators import (
    any_asset_name,
    any_dataset_id,
    any_dataset_version_id,
    any_hex_multihash,
)
from .stac_objects import MINIMAL_VALID_STAC_COLLECTION_OBJECT

LOGGER = logging.getLogger("backend.check_stac_metadata.utils")


def should_log_assets() -> None:
    base_url = any_s3_url()
    metadata_url = f"{base_url}/{any_safe_filename()}"
    stac_object = deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)
    asset_url = f"{base_url}/{any_safe_filename()}"
    asset_multihash = any_hex_multihash()
    stac_object["assets"] = {
        any_asset_name(): {
            "href": asset_url,
            "file:checksum": asset_multihash,
        },
    }

    url_reader = MockJSONURLReader({metadata_url: stac_object})
    expected_message = dumps({"asset": {"url": asset_url, "multihash": asset_multihash}})

    with patch.object(LOGGER, "debug") as logger_mock, patch(
        "backend.check_stac_metadata.utils.processing_assets_model_with_meta"
    ):
        STACDatasetValidator(url_reader, MockValidationResultFactory()).validate(metadata_url)

        logger_mock.assert_any_call(expected_message)


def should_log_non_s3_url_prefix_validation() -> None:
    metadata_url = any_https_url()
    hash_key = f"DATASET#{any_dataset_id()}#VERSION#{any_dataset_version_id()}"
    url_reader = MockJSONURLReader({metadata_url: MINIMAL_VALID_STAC_COLLECTION_OBJECT})
    expected_message = dumps(
        {"success": False, "message": f"URL doesn't start with “{S3_URL_PREFIX}”: “{metadata_url}”"}
    )

    with patch.object(LOGGER, "error") as logger_mock, patch(
        "backend.check_stac_metadata.utils.processing_assets_model_with_meta"
    ):
        STACDatasetValidator(url_reader, MockValidationResultFactory()).run(metadata_url, hash_key)

        logger_mock.assert_any_call(expected_message)


@patch("backend.check_stac_metadata.utils.STACDatasetValidator.validate")
def should_log_staging_access_validation(validate_mock: MagicMock) -> None:
    metadata_url = any_s3_url()
    hash_key = f"DATASET#{any_dataset_id()}#VERSION#{any_dataset_version_id()}"

    expected_error = ClientError(
        {"Error": {"Code": "TEST", "Message": "TEST"}}, operation_name="get_object"
    )
    validate_mock.side_effect = expected_error

    url_reader = MockJSONURLReader({metadata_url: MINIMAL_VALID_STAC_COLLECTION_OBJECT})

    expected_message = dumps({"success": False, "message": str(expected_error)})

    with patch.object(LOGGER, "error") as logger_mock, patch(
        "backend.check_stac_metadata.utils.processing_assets_model_with_meta"
    ):
        STACDatasetValidator(url_reader, MockValidationResultFactory()).run(metadata_url, hash_key)

        logger_mock.assert_any_call(expected_message)


@patch("backend.check_stac_metadata.utils.STACDatasetValidator.validate")
def should_log_schema_mismatch_validation(validate_mock: MagicMock) -> None:
    metadata_url = any_s3_url()
    hash_key = f"DATASET#{any_dataset_id()}#VERSION#{any_dataset_version_id()}"

    expected_error = ValidationError(any_error_message())
    validate_mock.side_effect = expected_error

    url_reader = MockJSONURLReader({metadata_url: MINIMAL_VALID_STAC_COLLECTION_OBJECT})

    expected_message = dumps({"success": False, "message": expected_error.message})

    with patch.object(LOGGER, "error") as logger_mock, patch(
        "backend.check_stac_metadata.utils.processing_assets_model_with_meta"
    ):
        STACDatasetValidator(url_reader, MockValidationResultFactory()).run(metadata_url, hash_key)

        logger_mock.assert_any_call(expected_message)


@patch("backend.check_stac_metadata.utils.STACDatasetValidator.validate")
def should_log_json_parse_validation(validate_mock: MagicMock) -> None:
    metadata_url = any_s3_url()
    hash_key = f"DATASET#{any_dataset_id()}#VERSION#{any_dataset_version_id()}"

    url_reader = MockJSONURLReader({metadata_url: StringIO(initial_value="{")})

    expected_error = JSONDecodeError(any_error_message(), "", 0)
    validate_mock.side_effect = expected_error

    expected_message = dumps({"success": False, "message": str(expected_error)})

    with patch.object(LOGGER, "error") as logger_mock, patch(
        "backend.check_stac_metadata.utils.processing_assets_model_with_meta"
    ):
        STACDatasetValidator(url_reader, MockValidationResultFactory()).run(metadata_url, hash_key)

        logger_mock.assert_any_call(expected_message)
