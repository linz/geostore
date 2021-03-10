from os import environ
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from botocore.response import StreamingBody  # type: ignore[import]
from multihash import FUNCS, decode  # type: ignore[import]

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_s3 import S3Client
else:
    # In production we want to avoid depending on a package which has no runtime impact
    S3Client = object

ARRAY_INDEX_VARIABLE_NAME = "AWS_BATCH_JOB_ARRAY_INDEX"

CHUNK_SIZE = 1024


class ChecksumMismatchError(Exception):
    def __init__(self, actual_hex_digest: str):
        super().__init__()

        self.actual_hex_digest = actual_hex_digest


def validate_url_multihash(url: str, hex_multihash: str, s3_client: S3Client) -> None:
    parsed_url = urlparse(url)
    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip("/")
    url_stream: StreamingBody = s3_client.get_object(Bucket=bucket, Key=key)["Body"]
    checksum_function_code = int(hex_multihash[:2], 16)
    checksum_function = FUNCS[checksum_function_code]

    file_digest = checksum_function()
    for chunk in url_stream.iter_chunks(chunk_size=CHUNK_SIZE):
        file_digest.update(chunk)

    if file_digest.digest() != decode(bytes.fromhex(hex_multihash)):
        raise ChecksumMismatchError(file_digest.hexdigest())


def get_job_offset() -> int:
    return int(environ.get(ARRAY_INDEX_VARIABLE_NAME, 0))
