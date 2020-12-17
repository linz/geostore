from io import BytesIO
from typing import Callable, Mapping

from botocore.auth import EMPTY_SHA256_HASH  # type: ignore[import]
from botocore.response import StreamingBody  # type: ignore[import]
from multihash import SHA2_256  # type: ignore[import]

from ..processing.check_files_checksums.task import validate_url_multihash
from .utils import any_s3_url

SHA256_BYTE_COUNT = len(EMPTY_SHA256_HASH) >> 1
EMPTY_FILE_MULTIHASH = f"{SHA2_256:x}{SHA256_BYTE_COUNT:x}{EMPTY_SHA256_HASH}"


def fake_url_reader(url_to_content: Mapping[str, bytes]) -> Callable[[str], StreamingBody]:
    def read_url(url: str) -> StreamingBody:
        content = url_to_content[url]
        content_stream = BytesIO(content)
        content_length = len(content)
        return StreamingBody(content_stream, content_length)

    return read_url


def test_should_return_true_when_empty_file_checksum_matches() -> None:
    url = any_s3_url()
    url_reader = fake_url_reader({url: b""})
    assert validate_url_multihash(url, EMPTY_FILE_MULTIHASH, url_reader)


def test_should_return_false_when_checksum_does_not_match() -> None:
    url = any_s3_url()
    url_reader = fake_url_reader({url: b""})
    checksum = "0" * 64
    checksum_byte_count = 32

    assert not validate_url_multihash(
        url, f"{SHA2_256:x}{checksum_byte_count:x}{checksum}", url_reader
    )
