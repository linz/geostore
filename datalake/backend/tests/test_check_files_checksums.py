import logging
import sys
from io import BytesIO
from unittest.mock import ANY, MagicMock, patch

from botocore.auth import EMPTY_SHA256_HASH  # type: ignore[import]
from botocore.response import StreamingBody  # type: ignore[import]
from botocore.stub import Stubber  # type: ignore[import]
from multihash import SHA2_256  # type: ignore[import]
from mypy_boto3_s3 import S3Client
from pytest import raises

from ..processing.check_files_checksums.task import (
    ChecksumMismatchError,
    main,
    validate_url_multihash,
)
from .utils import (
    any_hex_multihash,
    any_program_name,
    any_s3_url,
    any_sha256_hex_digest,
    sha256_hex_digest_to_multihash,
)

SHA256_BYTE_COUNT = len(EMPTY_SHA256_HASH) >> 1
EMPTY_FILE_MULTIHASH = f"{SHA2_256:x}{SHA256_BYTE_COUNT:x}{EMPTY_SHA256_HASH}"


def test_should_return_when_empty_file_checksum_matches(s3_client: S3Client) -> None:
    s3_stubber = Stubber(s3_client)
    s3_stubber.add_response("get_object", {"Body": StreamingBody(BytesIO(), 0)})
    with s3_stubber:
        validate_url_multihash(any_s3_url(), EMPTY_FILE_MULTIHASH, s3_client)


def test_should_raise_exception_when_checksum_does_not_match(s3_client: S3Client) -> None:
    s3_stubber = Stubber(s3_client)
    s3_stubber.add_response("get_object", {"Body": StreamingBody(BytesIO(), 0)})
    checksum = "0" * 64
    checksum_byte_count = 32

    with s3_stubber, raises(ChecksumMismatchError):
        validate_url_multihash(
            any_s3_url(), f"{SHA2_256:x}{checksum_byte_count:x}{checksum}", s3_client
        )


@patch("datalake.backend.processing.check_files_checksums.task.validate_url_multihash")
def test_should_validate_given_url_and_checksum(validate_url_multihash_mock: MagicMock) -> None:
    url = any_s3_url()
    hex_multihash = any_hex_multihash()
    sys.argv = [any_program_name(), f"--file-url={url}", f"--hex-multihash={hex_multihash}"]

    with patch("boto3.client"):
        assert main() == 0

    validate_url_multihash_mock.assert_called_once_with(url, hex_multihash, ANY)


@patch("datalake.backend.processing.check_files_checksums.task.validate_url_multihash")
def test_should_print_json_output_when_validation_succeeds(
    validate_url_multihash_mock: MagicMock,
) -> None:
    validate_url_multihash_mock.return_value = True
    logger = logging.getLogger("datalake.backend.processing.check_files_checksums.task")
    sys.argv = [
        any_program_name(),
        f"--file-url={any_s3_url()}",
        f"--hex-multihash={any_hex_multihash()}",
    ]

    with patch.object(logger, "info") as info_log_mock, patch("boto3.client"):
        main()

        info_log_mock.assert_called_with('{"success": true, "message": ""}')


@patch("datalake.backend.processing.check_files_checksums.task.validate_url_multihash")
def test_should_return_non_zero_exit_code_when_validation_fails(
    validate_url_multihash_mock: MagicMock,
) -> None:
    validate_url_multihash_mock.side_effect = ChecksumMismatchError(any_sha256_hex_digest())
    sys.argv = [
        any_program_name(),
        f"--file-url={any_s3_url()}",
        f"--hex-multihash={any_hex_multihash()}",
    ]

    assert main() == 1


@patch("datalake.backend.processing.check_files_checksums.task.validate_url_multihash")
def test_should_print_json_output_when_validation_fails(
    validate_url_multihash_mock: MagicMock,
) -> None:
    expected_hex_digest = any_sha256_hex_digest()
    actual_hex_digest = any_sha256_hex_digest()
    validate_url_multihash_mock.side_effect = ChecksumMismatchError(actual_hex_digest)
    expected_message = (
        '{"success": false, "message": "Checksum mismatch:'
        f' expected {expected_hex_digest}, got {actual_hex_digest}"}}'
    )
    logger = logging.getLogger("datalake.backend.processing.check_files_checksums.task")

    expected_multihash = sha256_hex_digest_to_multihash(expected_hex_digest)
    sys.argv = [
        any_program_name(),
        f"--file-url={any_s3_url()}",
        f"--hex-multihash={expected_multihash}",
    ]

    with patch.object(logger, "error") as error_log_mock, patch("boto3.client"):
        main()

        error_log_mock.assert_called_with(expected_message)
