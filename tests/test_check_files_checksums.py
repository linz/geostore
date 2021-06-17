import sys
from io import BytesIO
from json import dumps
from logging import Logger, getLogger
from os import environ
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, call, patch

from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from multihash import SHA2_256  # type: ignore[import]
from pytest import raises
from pytest_subtests import SubTests  # type: ignore[import]

from backend.api_keys import MESSAGE_KEY, SUCCESS_KEY
from backend.check import Check
from backend.check_files_checksums.task import main
from backend.check_files_checksums.utils import (
    ARRAY_INDEX_VARIABLE_NAME,
    CHUNK_SIZE,
    ChecksumMismatchError,
    ChecksumValidator,
    get_job_offset,
)
from backend.models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR, VERSION_ID_PREFIX
from backend.processing_assets_model import ProcessingAssetType, ProcessingAssetsModelBase
from backend.validation_results_model import ValidationResult

from .aws_utils import (
    MockValidationResultFactory,
    any_batch_job_array_index,
    any_s3_url,
    any_table_name,
)
from .general_generators import any_program_name
from .stac_generators import (
    any_dataset_id,
    any_dataset_version_id,
    any_hex_multihash,
    any_sha256_hex_digest,
    sha256_hex_digest_to_multihash,
)

if TYPE_CHECKING:
    from botocore.exceptions import (  # pylint:disable=no-name-in-module,ungrouped-imports
        ClientErrorResponseError,
        ClientErrorResponseTypeDef,
    )
else:
    ClientErrorResponseError = ClientErrorResponseTypeDef = dict

SHA256_CHECKSUM_BYTE_COUNT = 32


def should_return_offset_from_array_index_variable() -> None:
    index = any_batch_job_array_index()
    with patch.dict(environ, {ARRAY_INDEX_VARIABLE_NAME: str(index)}):

        assert get_job_offset() == index


def should_return_default_offset_to_zero() -> None:
    environ.pop(ARRAY_INDEX_VARIABLE_NAME, default=None)

    assert get_job_offset() == 0


@patch("backend.check_files_checksums.utils.ChecksumValidator.validate_url_multihash")
@patch("backend.check_files_checksums.utils.processing_assets_model_with_meta")
@patch("backend.check_files_checksums.task.ValidationResultFactory")
def should_validate_given_index(
    validation_results_factory_mock: MagicMock,
    processing_assets_model_mock: MagicMock,
    validate_url_multihash_mock: MagicMock,
    subtests: SubTests,
) -> None:
    # Given
    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()
    hash_key = f"{DATASET_ID_PREFIX}{dataset_id}{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{version_id}"

    url = any_s3_url()
    hex_multihash = any_hex_multihash()

    array_index = "1"

    def get_mock(given_hash_key: str, range_key: str) -> ProcessingAssetsModelBase:
        assert given_hash_key == hash_key
        assert range_key == f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}{array_index}"
        return ProcessingAssetsModelBase(
            hash_key=given_hash_key,
            range_key=f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}1",
            url=url,
            multihash=hex_multihash,
        )

    processing_assets_model_mock.return_value.get.side_effect = get_mock
    logger = getLogger("backend.check_files_checksums.task")
    validation_results_table_name = any_table_name()
    expected_calls = [
        call(hash_key, validation_results_table_name),
        call().save(url, Check.CHECKSUM, ValidationResult.PASSED),
    ]

    # When
    sys.argv = [
        any_program_name(),
        f"--dataset-id={dataset_id}",
        f"--version-id={version_id}",
        f"--assets-table-name={any_table_name()}",
        f"--results-table-name={validation_results_table_name}",
        "--first-item=0",
    ]
    with patch.object(logger, "info") as info_log_mock, patch.dict(
        environ, {ARRAY_INDEX_VARIABLE_NAME: array_index}
    ):
        # Then
        main()

        with subtests.test(msg="Log message"):
            info_log_mock.assert_any_call(dumps({SUCCESS_KEY: True, MESSAGE_KEY: ""}))

    with subtests.test(msg="Validate checksums"):
        assert validate_url_multihash_mock.mock_calls == [call(url, hex_multihash)]

    with subtests.test(msg="Validation result"):
        assert validation_results_factory_mock.mock_calls == expected_calls


@patch("backend.check_files_checksums.utils.ChecksumValidator.validate_url_multihash")
@patch("backend.check_files_checksums.utils.processing_assets_model_with_meta")
@patch("backend.check_files_checksums.task.ValidationResultFactory")
def should_log_error_when_validation_fails(  # pylint: disable=too-many-locals
    validation_results_factory_mock: MagicMock,
    processing_assets_model_mock: MagicMock,
    validate_url_multihash_mock: MagicMock,
    subtests: SubTests,
) -> None:

    # Given
    actual_hex_digest = any_sha256_hex_digest()
    expected_hex_digest = any_sha256_hex_digest()
    expected_hex_multihash = sha256_hex_digest_to_multihash(expected_hex_digest)
    dataset_id = any_dataset_id()
    dataset_version_id = any_dataset_version_id()
    hash_key = (
        f"{DATASET_ID_PREFIX}{dataset_id}{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{dataset_version_id}"
    )
    url = any_s3_url()
    processing_assets_model_mock.return_value.get.return_value = ProcessingAssetsModelBase(
        hash_key=hash_key,
        range_key=f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}0",
        url=url,
        multihash=expected_hex_multihash,
    )
    expected_details = {
        MESSAGE_KEY: f"Checksum mismatch: expected {expected_hex_digest}, got {actual_hex_digest}"
    }
    expected_log = dumps({SUCCESS_KEY: False, **expected_details})
    validate_url_multihash_mock.side_effect = ChecksumMismatchError(actual_hex_digest)
    logger = getLogger("backend.check_files_checksums.task")
    # When

    validation_results_table_name = any_table_name()
    sys.argv = [
        any_program_name(),
        f"--dataset-id={dataset_id}",
        f"--version-id={dataset_version_id}",
        f"--assets-table-name={any_table_name()}",
        f"--results-table-name={validation_results_table_name}",
        "--first-item=0",
    ]

    # Then
    with patch.object(logger, "error") as error_log_mock, patch.dict(
        environ, {ARRAY_INDEX_VARIABLE_NAME: "0"}
    ):
        main()

        with subtests.test(msg="Log message"):
            error_log_mock.assert_any_call(expected_log)

    with subtests.test(msg="Validation result"):
        assert validation_results_factory_mock.mock_calls == [
            call(hash_key, validation_results_table_name),
            call().save(url, Check.CHECKSUM, ValidationResult.FAILED, details=expected_details),
        ]


@patch("backend.check_files_checksums.utils.S3_CLIENT.get_object")
@patch("backend.check_files_checksums.utils.processing_assets_model_with_meta")
@patch("backend.check_files_checksums.task.ValidationResultFactory")
def should_save_staging_access_validation_results(
    validation_results_factory_mock: MagicMock,
    processing_assets_model_mock: MagicMock,
    get_object_mock: MagicMock,
) -> None:
    expected_error = ClientError(
        ClientErrorResponseTypeDef(Error=ClientErrorResponseError(Code="TEST", Message="TEST")),
        operation_name="get_object",
    )
    get_object_mock.side_effect = expected_error

    s3_url = any_s3_url()
    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()
    hash_key = f"{DATASET_ID_PREFIX}{dataset_id}{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{version_id}"

    array_index = "1"

    validation_results_table_name = any_table_name()
    # When
    sys.argv = [
        any_program_name(),
        f"--dataset-id={dataset_id}",
        f"--version-id={version_id}",
        f"--assets-table-name={any_table_name()}",
        f"--results-table-name={validation_results_table_name}",
        "--first-item=0",
    ]

    def get_mock(given_hash_key: str, range_key: str) -> ProcessingAssetsModelBase:
        assert given_hash_key == hash_key
        assert range_key == f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}{array_index}"
        return ProcessingAssetsModelBase(
            hash_key=given_hash_key,
            range_key=f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}1",
            url=s3_url,
            multihash=any_hex_multihash(),
        )

    processing_assets_model_mock.return_value.get.side_effect = get_mock

    with raises(ClientError), patch.dict(environ, {ARRAY_INDEX_VARIABLE_NAME: array_index}):
        main()

    assert validation_results_factory_mock.mock_calls == [
        call(hash_key, validation_results_table_name),
        call().save(
            s3_url,
            Check.STAGING_ACCESS,
            ValidationResult.FAILED,
            details={MESSAGE_KEY: str(expected_error)},
        ),
    ]


class TestsWithLogger:
    logger: Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = getLogger("backend.check_files_checksums.task")

    @patch("backend.check_files_checksums.utils.S3_CLIENT.get_object")
    def should_return_when_file_checksum_matches(self, get_object_mock: MagicMock) -> None:
        file_contents = b"x" * (CHUNK_SIZE + 1)
        get_object_mock.return_value = {
            "Body": StreamingBody(BytesIO(initial_bytes=file_contents), len(file_contents))
        }
        multihash = (
            f"{SHA2_256:x}{SHA256_CHECKSUM_BYTE_COUNT:x}"
            "c6d8e9905300876046729949cc95c2385221270d389176f7234fe7ac00c4e430"
        )

        with patch("backend.check_files_checksums.utils.processing_assets_model_with_meta"):
            ChecksumValidator(
                any_table_name(), MockValidationResultFactory(), self.logger
            ).validate_url_multihash(any_s3_url(), multihash)

    @patch("backend.check_files_checksums.utils.S3_CLIENT.get_object")
    def should_raise_exception_when_checksum_does_not_match(
        self, get_object_mock: MagicMock
    ) -> None:
        get_object_mock.return_value = {"Body": StreamingBody(BytesIO(), 0)}

        checksum = "0" * 64
        with raises(ChecksumMismatchError), patch(
            "backend.check_files_checksums.utils.processing_assets_model_with_meta"
        ):
            ChecksumValidator(
                any_table_name(), MockValidationResultFactory(), self.logger
            ).validate_url_multihash(
                any_s3_url(), f"{SHA2_256:x}{SHA256_CHECKSUM_BYTE_COUNT:x}{checksum}"
            )
