import sys
from io import BytesIO
from os import environ
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, call, patch

from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from multihash import SHA2_256
from pytest import raises
from pytest_subtests import SubTests

from geostore.api_keys import MESSAGE_KEY
from geostore.check import Check
from geostore.check_files_checksums.task import main
from geostore.check_files_checksums.utils import (
    ARRAY_INDEX_VARIABLE_NAME,
    ChecksumMismatchError,
    ChecksumValidator,
    get_job_offset,
)
from geostore.logging_keys import LOG_MESSAGE_VALIDATION_COMPLETE
from geostore.models import DB_KEY_SEPARATOR
from geostore.processing_assets_model import ProcessingAssetType, ProcessingAssetsModelBase
from geostore.s3 import CHUNK_SIZE
from geostore.step_function import Outcome, get_hash_key
from geostore.validation_results_model import ValidationResult

from .aws_utils import (
    MockGeostoreS3Response,
    MockJSONURLReader,
    MockValidationResultFactory,
    any_batch_job_array_index,
    any_role_arn,
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
    from botocore.exceptions import ClientErrorResponseError, ClientErrorResponseTypeDef
else:
    ClientErrorResponseError = ClientErrorResponseTypeDef = dict

SHA256_CHECKSUM_BYTE_COUNT = 32


def should_return_offset_from_array_index_variable() -> None:
    index = any_batch_job_array_index()
    with patch.dict(environ, {ARRAY_INDEX_VARIABLE_NAME: str(index)}):
        assert get_job_offset() == index


def should_return_default_offset_to_zero() -> None:
    environ.pop(ARRAY_INDEX_VARIABLE_NAME, default="")

    assert get_job_offset() == 0


@patch("geostore.check_files_checksums.utils.ChecksumValidator.validate_url_multihash")
@patch("geostore.check_files_checksums.utils.processing_assets_model_with_meta")
@patch("geostore.check_files_checksums.task.ValidationResultFactory")
def should_validate_given_index(
    validation_results_factory_mock: MagicMock,
    processing_assets_model_mock: MagicMock,
    validate_url_multihash_mock: MagicMock,
    subtests: SubTests,
) -> None:
    # Given
    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()
    hash_key = get_hash_key(dataset_id, version_id)

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
        "--first-item=0",
        f"--assets-table-name={any_table_name()}",
        f"--results-table-name={validation_results_table_name}",
        f"--s3-role-arn={any_role_arn()}",
    ]
    with patch("geostore.check_files_checksums.task.LOGGER.info") as info_log_mock, patch.dict(
        environ, {ARRAY_INDEX_VARIABLE_NAME: array_index}
    ), patch("geostore.check_files_checksums.task.get_s3_url_reader"):
        # Then
        main()

        with subtests.test(msg="Log message"):
            info_log_mock.assert_any_call(
                LOG_MESSAGE_VALIDATION_COMPLETE, extra={"outcome": Outcome.PASSED}
            )

    with subtests.test(msg="Validate checksums"):
        assert validate_url_multihash_mock.mock_calls == [call(url, hex_multihash)]

    with subtests.test(msg="Validation result"):
        assert validation_results_factory_mock.mock_calls == expected_calls


@patch("geostore.check_files_checksums.utils.ChecksumValidator.validate_url_multihash")
@patch("geostore.check_files_checksums.utils.processing_assets_model_with_meta")
@patch("geostore.check_files_checksums.task.ValidationResultFactory")
def should_log_error_when_validation_fails(
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
    hash_key = get_hash_key(dataset_id, dataset_version_id)
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
    validate_url_multihash_mock.side_effect = ChecksumMismatchError(actual_hex_digest)
    # When

    validation_results_table_name = any_table_name()
    sys.argv = [
        any_program_name(),
        f"--dataset-id={dataset_id}",
        f"--version-id={dataset_version_id}",
        "--first-item=0",
        f"--assets-table-name={any_table_name()}",
        f"--results-table-name={validation_results_table_name}",
        f"--s3-role-arn={any_role_arn()}",
    ]

    # Then
    with patch("geostore.check_files_checksums.task.LOGGER.error") as error_log_mock, patch.dict(
        environ, {ARRAY_INDEX_VARIABLE_NAME: "0"}
    ), patch("geostore.check_files_checksums.task.get_s3_url_reader"):
        main()

        with subtests.test(msg="Log message"):
            error_log_mock.assert_any_call(
                LOG_MESSAGE_VALIDATION_COMPLETE,
                extra={"outcome": Outcome.FAILED, "error": expected_details},
            )

    with subtests.test(msg="Validation result"):
        assert validation_results_factory_mock.mock_calls == [
            call(hash_key, validation_results_table_name),
            call().save(url, Check.CHECKSUM, ValidationResult.FAILED, details=expected_details),
        ]


@patch("geostore.check_files_checksums.task.get_s3_url_reader")
@patch("geostore.check_files_checksums.utils.processing_assets_model_with_meta")
@patch("geostore.check_files_checksums.task.ValidationResultFactory")
def should_save_staging_access_validation_results(
    validation_results_factory_mock: MagicMock,
    processing_assets_model_mock: MagicMock,
    get_s3_url_reader: MagicMock,
) -> None:
    expected_error = ClientError(
        ClientErrorResponseTypeDef(Error=ClientErrorResponseError(Code="TEST", Message="TEST")),
        operation_name="get_object",
    )
    get_s3_url_reader.return_value.side_effect = expected_error

    s3_url = any_s3_url()
    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()
    hash_key = get_hash_key(dataset_id, version_id)

    array_index = "1"

    validation_results_table_name = any_table_name()
    # When
    sys.argv = [
        any_program_name(),
        f"--dataset-id={dataset_id}",
        f"--version-id={version_id}",
        "--first-item=0",
        f"--assets-table-name={any_table_name()}",
        f"--results-table-name={validation_results_table_name}",
        f"--s3-role-arn={any_role_arn()}",
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


def should_return_when_file_checksum_matches() -> None:
    file_contents = b"x" * (CHUNK_SIZE + 1)
    url = any_s3_url()
    s3_url_reader = MockJSONURLReader(
        {
            url: MockGeostoreS3Response(
                StreamingBody(BytesIO(initial_bytes=file_contents), len(file_contents))
            )
        }
    )

    multihash = (
        f"{SHA2_256:x}{SHA256_CHECKSUM_BYTE_COUNT:x}"
        "c6d8e9905300876046729949cc95c2385221270d389176f7234fe7ac00c4e430"
    )

    with patch("geostore.check_files_checksums.utils.processing_assets_model_with_meta"):
        ChecksumValidator(
            any_table_name(), MockValidationResultFactory(), s3_url_reader, MagicMock()
        ).validate_url_multihash(url, multihash)


def should_raise_exception_when_checksum_does_not_match() -> None:
    url = any_s3_url()
    s3_url_reader = MockJSONURLReader({url: MockGeostoreS3Response(StreamingBody(BytesIO(), 0))})

    checksum = "0" * 64
    with raises(ChecksumMismatchError), patch(
        "geostore.check_files_checksums.utils.processing_assets_model_with_meta"
    ):
        ChecksumValidator(
            any_table_name(), MockValidationResultFactory(), s3_url_reader, MagicMock()
        ).validate_url_multihash(url, f"{SHA2_256:x}{SHA256_CHECKSUM_BYTE_COUNT:x}{checksum}")
