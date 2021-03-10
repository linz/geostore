#!/usr/bin/env python3
import logging
import sys
from argparse import ArgumentParser, Namespace
from json import dumps
from typing import Any, Mapping

import boto3

from ..log import set_up_logging
from ..processing_assets_model import ProcessingAssetsModel
from .utils import ChecksumMismatchError, get_job_offset, validate_url_multihash


def parse_arguments() -> Namespace:
    argument_parser = ArgumentParser()
    argument_parser.add_argument("--dataset-id", required=True)
    argument_parser.add_argument("--version-id", required=True)
    argument_parser.add_argument("--first-item", type=int, required=True)
    return argument_parser.parse_args()


def success(logger: logging.Logger) -> int:
    logger.info(dumps({"success": True, "message": ""}))
    return 0


def failure(content: Mapping[str, Any], logger: logging.Logger) -> int:
    logger.error(dumps({"success": False, **content}))
    return 0


def main() -> int:
    logger = set_up_logging(__name__)

    arguments = parse_arguments()
    s3_client = boto3.client("s3")

    index = arguments.first_item + get_job_offset()
    hash_key = f"DATASET#{arguments.dataset_id}#VERSION#{arguments.version_id}"
    range_key = f"DATA_ITEM_INDEX#{index}"

    try:
        item = ProcessingAssetsModel.get(hash_key, range_key=range_key)
    except ProcessingAssetsModel.DoesNotExist as error:
        return failure(
            {
                "error": {"message": error.msg, "cause": error.cause},
                "parameters": {"hash_key": hash_key, "range_key": range_key},
            },
            logger,
        )

    try:
        validate_url_multihash(item.url, item.multihash, s3_client)
    except ChecksumMismatchError as error:
        content = {
            "message": f"Checksum mismatch: expected {item.multihash[4:]},"
            f" got {error.actual_hex_digest}"
        }
        return failure(content, logger)

    return success(logger)


if __name__ == "__main__":
    sys.exit(main())
