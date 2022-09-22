from copy import deepcopy
from io import BytesIO
from json import JSONDecodeError
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from jsonschema import ValidationError
from pytest import raises

from geostore.check_stac_metadata.utils import (
    LOG_MESSAGE_STAC_ASSET_INFO,
    PROCESSING_ASSET_MULTIHASH_KEY,
    PROCESSING_ASSET_URL_KEY,
    InvalidSTACRootTypeError,
    STACDatasetValidator,
)
from geostore.logging_keys import GIT_COMMIT, LOG_MESSAGE_VALIDATION_COMPLETE
from geostore.parameter_store import ParameterName, get_param
from geostore.s3 import S3_URL_PREFIX
from geostore.stac_format import (
    LINZ_STAC_CREATED_KEY,
    LINZ_STAC_UPDATED_KEY,
    STAC_ASSETS_KEY,
    STAC_FILE_CHECKSUM_KEY,
    STAC_HREF_KEY,
)
from geostore.step_function import Outcome

from .aws_utils import (
    MockAssetGarbageCollector,
    MockGeostoreS3Response,
    MockJSONURLReader,
    MockValidationResultFactory,
    any_s3_url,
)
from .dynamodb_generators import any_hash_key
from .general_generators import (
    any_error_message,
    any_https_url,
    any_past_datetime_string,
    any_safe_filename,
)
from .stac_generators import any_asset_name, any_hex_multihash
from .stac_objects import MINIMAL_VALID_STAC_COLLECTION_OBJECT

if TYPE_CHECKING:
    from botocore.exceptions import ClientErrorResponseError, ClientErrorResponseTypeDef
else:
    ClientErrorResponseError = ClientErrorResponseTypeDef = dict


def should_log_assets() -> None:
    base_url = any_s3_url()
    metadata_url = f"{base_url}/{any_safe_filename()}"
    stac_object = deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)
    asset_url = f"{base_url}/{any_safe_filename()}"
    asset_multihash = any_hex_multihash()
    stac_object[STAC_ASSETS_KEY] = {
        any_asset_name(): {
            LINZ_STAC_CREATED_KEY: any_past_datetime_string(),
            LINZ_STAC_UPDATED_KEY: any_past_datetime_string(),
            STAC_HREF_KEY: asset_url,
            STAC_FILE_CHECKSUM_KEY: asset_multihash,
        },
    }

    url_reader = MockJSONURLReader(
        {metadata_url: MockGeostoreS3Response(stac_object, file_in_staging=True)}
    )

    with patch("geostore.check_stac_metadata.utils.LOGGER.debug") as logger_mock, patch(
        "geostore.check_stac_metadata.utils.processing_assets_model_with_meta"
    ):
        STACDatasetValidator(
            any_hash_key(), url_reader, MockAssetGarbageCollector(), MockValidationResultFactory()
        ).validate(metadata_url)

        logger_mock.assert_any_call(
            LOG_MESSAGE_STAC_ASSET_INFO,
            extra={
                "asset": {
                    PROCESSING_ASSET_URL_KEY: asset_url,
                    PROCESSING_ASSET_MULTIHASH_KEY: asset_multihash,
                },
                GIT_COMMIT: get_param(ParameterName.GIT_COMMIT),
            },
        )


def should_log_non_s3_url_prefix_validation() -> None:
    metadata_url = any_https_url()
    hash_key = any_hash_key()
    url_reader = MockJSONURLReader(
        {
            metadata_url: MockGeostoreS3Response(
                MINIMAL_VALID_STAC_COLLECTION_OBJECT, file_in_staging=True
            )
        }
    )

    with patch("geostore.check_stac_metadata.utils.LOGGER.error") as logger_mock, patch(
        "geostore.check_stac_metadata.utils.processing_assets_model_with_meta"
    ), raises(InvalidSTACRootTypeError):
        STACDatasetValidator(
            hash_key, url_reader, MockAssetGarbageCollector(), MockValidationResultFactory()
        ).run(metadata_url)

        logger_mock.assert_any_call(
            LOG_MESSAGE_VALIDATION_COMPLETE,
            extra={
                "outcome": Outcome.FAILED,
                "error": f"URL doesn't start with “{S3_URL_PREFIX}”: “{metadata_url}”",
                GIT_COMMIT: get_param(ParameterName.GIT_COMMIT),
            },
        )


@patch("geostore.check_stac_metadata.utils.STACDatasetValidator.validate")
def should_log_staging_access_validation(validate_mock: MagicMock) -> None:
    metadata_url = any_s3_url()
    hash_key = any_hash_key()

    expected_error = ClientError(
        ClientErrorResponseTypeDef(Error=ClientErrorResponseError(Code="TEST", Message="TEST")),
        operation_name="get_object",
    )
    validate_mock.side_effect = expected_error

    url_reader = MockJSONURLReader(
        {
            metadata_url: MockGeostoreS3Response(
                MINIMAL_VALID_STAC_COLLECTION_OBJECT, file_in_staging=True
            )
        }
    )

    with patch("geostore.check_stac_metadata.utils.LOGGER.error") as logger_mock, patch(
        "geostore.check_stac_metadata.utils.processing_assets_model_with_meta"
    ):
        STACDatasetValidator(
            hash_key, url_reader, MockAssetGarbageCollector(), MockValidationResultFactory()
        ).run(metadata_url)

        logger_mock.assert_any_call(
            LOG_MESSAGE_VALIDATION_COMPLETE,
            extra={
                "outcome": Outcome.FAILED,
                "error": str(expected_error),
                GIT_COMMIT: get_param(ParameterName.GIT_COMMIT),
            },
        )


@patch("geostore.check_stac_metadata.utils.STACDatasetValidator.validate")
def should_log_schema_mismatch_validation(validate_mock: MagicMock) -> None:
    metadata_url = any_s3_url()
    hash_key = any_hash_key()

    expected_error = ValidationError(any_error_message())
    validate_mock.side_effect = expected_error

    url_reader = MockJSONURLReader(
        {
            metadata_url: MockGeostoreS3Response(
                MINIMAL_VALID_STAC_COLLECTION_OBJECT, file_in_staging=True
            )
        }
    )

    with patch("geostore.check_stac_metadata.utils.LOGGER.error") as logger_mock, patch(
        "geostore.check_stac_metadata.utils.processing_assets_model_with_meta"
    ):
        STACDatasetValidator(
            hash_key, url_reader, MockAssetGarbageCollector(), MockValidationResultFactory()
        ).run(metadata_url)

        logger_mock.assert_any_call(
            LOG_MESSAGE_VALIDATION_COMPLETE,
            extra={
                "outcome": Outcome.FAILED,
                "error": str(expected_error),
                GIT_COMMIT: get_param(ParameterName.GIT_COMMIT),
            },
        )


@patch("geostore.check_stac_metadata.utils.STACDatasetValidator.validate")
def should_log_json_parse_validation(validate_mock: MagicMock) -> None:
    metadata_url = any_s3_url()
    hash_key = any_hash_key()

    file_contents = b"{"
    url_reader = MockJSONURLReader(
        {
            metadata_url: MockGeostoreS3Response(
                StreamingBody(BytesIO(initial_bytes=file_contents), len(file_contents)),
                file_in_staging=True,
            )
        }
    )

    expected_error = JSONDecodeError(any_error_message(), "", 0)
    validate_mock.side_effect = expected_error

    with patch("geostore.check_stac_metadata.utils.LOGGER.error") as logger_mock, patch(
        "geostore.check_stac_metadata.utils.processing_assets_model_with_meta"
    ):
        STACDatasetValidator(
            hash_key, url_reader, MockAssetGarbageCollector(), MockValidationResultFactory()
        ).run(metadata_url)

        logger_mock.assert_any_call(
            LOG_MESSAGE_VALIDATION_COMPLETE,
            extra={
                "outcome": Outcome.FAILED,
                "error": str(expected_error),
                GIT_COMMIT: get_param(ParameterName.GIT_COMMIT),
            },
        )
