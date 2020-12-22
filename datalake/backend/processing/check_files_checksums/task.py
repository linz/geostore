#!/usr/bin/env python3
import sys
from argparse import ArgumentParser, Namespace
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import boto3
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


def validate_url_multihash(url: str, hex_multihash: str, s3_client: S3Client) -> bool:
    parsed_url = urlparse(url)
    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip("/")
    url_stream: StreamingBody = s3_client.get_object(Bucket=bucket, Key=key)["Body"]
    checksum_function_code = int(hex_multihash[:2], 16)
    checksum_function = FUNCS[checksum_function_code]

    file_digest = checksum_function()
    for chunk in url_stream.iter_chunks(chunk_size=CHUNK_SIZE):
        file_digest.update(chunk)

    actual_digest: bytes = file_digest.digest()
    expected_digest: bytes = decode(bytes.fromhex(hex_multihash))
    return actual_digest == expected_digest


def parse_arguments() -> Namespace:
    argument_parser = ArgumentParser()
    argument_parser.add_argument("--file-url", required=True)
    argument_parser.add_argument("--hex-multihash", required=True)
    return argument_parser.parse_args()


def main() -> int:
    arguments = parse_arguments()
    s3_client = boto3.client("s3")

    if validate_url_multihash(arguments.file_url, arguments.hex_multihash, s3_client):
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
