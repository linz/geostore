#!/usr/bin/env python3
import logging
import sys
from argparse import ArgumentParser, Namespace
from json import dumps
from os import environ
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import boto3
from botocore.response import StreamingBody  # type: ignore[import]
from multihash import FUNCS, decode  # type: ignore[import]

from ..assets_model import ProcessingAssetsModel

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


def parse_arguments() -> Namespace:
    argument_parser = ArgumentParser()
    argument_parser.add_argument("--dataset-id", required=True)
    argument_parser.add_argument("--version-id", required=True)
    argument_parser.add_argument("--first-item", type=int, required=True)
    return argument_parser.parse_args()


def set_up_logging() -> logging.Logger:
    logger = logging.getLogger(__name__)

    log_handler = logging.StreamHandler()
    log_level = environ.get("LOGLEVEL", logging.NOTSET)

    logger.addHandler(log_handler)
    logger.setLevel(log_level)

    return logger


def success(logger: logging.Logger) -> int:
    logger.info(dumps({"success": True, "message": ""}))
    return 0


def main() -> int:
    logger = set_up_logging()

    arguments = parse_arguments()
    s3_client = boto3.client("s3")

    index = arguments.first_item + int(environ[ARRAY_INDEX_VARIABLE_NAME])
    hash_key = f"DATASET#{arguments.dataset_id}#VERSION#{arguments.version_id}"
    range_key = f"DATA_ITEM_INDEX#{index}"

    try:
        item = ProcessingAssetsModel.get(hash_key, range_key=range_key)
    except ProcessingAssetsModel.DoesNotExist as error:
        logger.warning(
            dumps(
                {
                    "error": {"message": error.msg, "cause": error.cause},
                    "parameters": {"hash_key": hash_key, "range_key": range_key},
                }
            )
        )
        return success(logger)

    try:
        validate_url_multihash(item.url, item.multihash, s3_client)
    except ChecksumMismatchError as error:
        logger.error(
            dumps(
                {
                    "success": False,
                    "message": f"Checksum mismatch: expected {item.multihash[4:]},"
                    f" got {error.actual_hex_digest}",
                }
            )
        )
        return 1

    return success(logger)


if __name__ == "__main__":
    sys.exit(main())
