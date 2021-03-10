import logging
import sys
from io import BytesIO
from os import environ
from unittest.mock import MagicMock, call, patch

import boto3
from botocore.response import StreamingBody  # type: ignore[import]
from botocore.stub import Stubber  # type: ignore[import]
from multihash import SHA2_256  # type: ignore[import]
from mypy_boto3_s3 import S3Client
from pytest import raises
from pytest_subtests import SubTests  # type: ignore[import]

from backend.check_files_checksums.task import main
from backend.check_files_checksums.utils import (
    ARRAY_INDEX_VARIABLE_NAME,
    ChecksumMismatchError,
    get_job_offset,
    validate_url_multihash,
)
from backend.processing_assets_model import ProcessingAssetsModel

from .aws_utils import EMPTY_FILE_MULTIHASH, any_batch_job_array_index, any_s3_url
from .general_generators import any_program_name
from .stac_generators import (
    any_dataset_id,
    any_dataset_version_id,
    any_hex_multihash,
    any_sha256_hex_digest,
    sha256_hex_digest_to_multihash,
)


def should_return_offset_from_array_index_variable() -> None:
    index = any_batch_job_array_index()
    environ[ARRAY_INDEX_VARIABLE_NAME] = str(index)

    assert get_job_offset() == index


def should_return_default_offset_to_zero() -> None:
    environ.pop(ARRAY_INDEX_VARIABLE_NAME, default=None)

    assert get_job_offset() == 0


def should_return_when_empty_file_checksum_matches(s3_client: S3Client) -> None:
    s3_stubber = Stubber(s3_client)
    s3_stubber.add_response("get_object", {"Body": StreamingBody(BytesIO(), 0)})
    with s3_stubber:
        validate_url_multihash(any_s3_url(), EMPTY_FILE_MULTIHASH, s3_client)


def should_raise_exception_when_checksum_does_not_match(s3_client: S3Client) -> None:
    s3_stubber = Stubber(s3_client)
    s3_stubber.add_response("get_object", {"Body": StreamingBody(BytesIO(), 0)})
    checksum = "0" * 64
    checksum_byte_count = 32

    with s3_stubber, raises(ChecksumMismatchError):
        validate_url_multihash(
            any_s3_url(), f"{SHA2_256:x}{checksum_byte_count:x}{checksum}", s3_client
        )


@patch("boto3.client")
@patch("backend.check_files_checksums.task.validate_url_multihash")
@patch("backend.check_files_checksums.task.ProcessingAssetsModel")
def should_validate_given_index(
    processing_assets_model_mock: MagicMock,
    validate_url_multihash_mock: MagicMock,
    s3_client_mock: MagicMock,
    subtests: SubTests,
) -> None:
    # Given
    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    url = any_s3_url()
    hex_multihash = any_hex_multihash()

    array_index = "1"

    def get_mock(hash_key: str, range_key: str) -> ProcessingAssetsModel:
        assert hash_key == f"DATASET#{dataset_id}#VERSION#{version_id}"
        assert range_key == f"DATA_ITEM_INDEX#{array_index}"
        return ProcessingAssetsModel(
            hash_key=f"DATASET#{dataset_id}#VERSION#{version_id}",
            range_key="DATA_ITEM_INDEX#1",
            url=url,
            multihash=hex_multihash,
        )

    processing_assets_model_mock.get.side_effect = get_mock
    logger = logging.getLogger("backend.check_files_checksums.task")

    # When
    environ[ARRAY_INDEX_VARIABLE_NAME] = array_index
    sys.argv = [
        any_program_name(),
        f"--dataset-id={dataset_id}",
        f"--version-id={version_id}",
        "--first-item=0",
    ]
    s3_client = boto3.client("s3")
    with patch.object(logger, "info") as info_log_mock, Stubber(s3_client) as stubber:
        s3_client_mock.return_value = stubber

        # Then
        with subtests.test(msg="Return code"):
            assert main() == 0

        with subtests.test(msg="Log message"):
            info_log_mock.assert_any_call('{"success": true, "message": ""}')

    with subtests.test(msg="Validate checksums"):
        validate_url_multihash_mock.assert_has_calls([call(url, hex_multihash, stubber)])


@patch("backend.check_files_checksums.task.validate_url_multihash")
@patch("backend.check_files_checksums.task.ProcessingAssetsModel")
def should_log_error_when_validation_fails(
    processing_assets_model_mock: MagicMock,
    validate_url_multihash_mock: MagicMock,
    subtests: SubTests,
) -> None:
    # Given
    actual_hex_digest = any_sha256_hex_digest()
    expected_hex_digest = any_sha256_hex_digest()
    expected_hex_multihash = sha256_hex_digest_to_multihash(expected_hex_digest)
    processing_assets_model_mock.get.return_value = ProcessingAssetsModel(
        hash_key=f"DATASET#{any_dataset_id()}#VERSION#{any_dataset_version_id()}",
        range_key="DATA_ITEM_INDEX#0",
        url=any_s3_url(),
        multihash=expected_hex_multihash,
    )
    expected_error_message = (
        '{"success": false, "message": "Checksum mismatch:'
        f' expected {expected_hex_digest}, got {actual_hex_digest}"}}'
    )
    validate_url_multihash_mock.side_effect = ChecksumMismatchError(actual_hex_digest)
    logger = logging.getLogger("backend.check_files_checksums.task")

    # When
    environ[ARRAY_INDEX_VARIABLE_NAME] = "0"
    sys.argv = [
        any_program_name(),
        f"--dataset-id={any_dataset_id()}",
        f"--version-id={any_dataset_version_id()}",
        "--first-item=0",
    ]

    # Then
    with patch.object(logger, "error") as error_log_mock:
        with subtests.test(msg="Return code"):
            assert main() == 0

        with subtests.test(msg="Log message"):
            error_log_mock.assert_any_call(expected_error_message)
