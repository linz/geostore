#!/usr/bin/env python3
import sys
from argparse import ArgumentParser, Namespace
from json import dumps

import boto3

from ..check import Check
from ..log import set_up_logging
from ..processing_assets_model import ProcessingAssetType, ProcessingAssetsModel
from ..types import JsonObject
from ..validation_results_model import ValidationResult, ValidationResultFactory
from .utils import ChecksumMismatchError, get_job_offset, validate_url_multihash

LOGGER = set_up_logging(__name__)

S3_CLIENT = boto3.client("s3")


def parse_arguments() -> Namespace:
    argument_parser = ArgumentParser()
    argument_parser.add_argument("--dataset-id", required=True)
    argument_parser.add_argument("--version-id", required=True)
    argument_parser.add_argument("--first-item", type=int, required=True)
    return argument_parser.parse_args()


def log_failure(content: JsonObject) -> None:
    LOGGER.error(dumps({"success": False, **content}))


def main() -> int:
    arguments = parse_arguments()

    index = arguments.first_item + get_job_offset()
    hash_key = f"DATASET#{arguments.dataset_id}#VERSION#{arguments.version_id}"
    range_key = f"{ProcessingAssetType.DATA.value}#{index}"

    try:
        item = ProcessingAssetsModel.get(hash_key, range_key=range_key)
    except ProcessingAssetsModel.DoesNotExist as error:
        log_failure(
            {
                "error": {"message": error.msg, "cause": error.cause},
                "parameters": {"hash_key": hash_key, "range_key": range_key},
            },
        )
        return 1

    validation_result_factory = ValidationResultFactory(hash_key)
    try:
        validate_url_multihash(item.url, item.multihash, S3_CLIENT)
    except ChecksumMismatchError as error:
        content = {
            "message": f"Checksum mismatch: expected {item.multihash[4:]},"
            f" got {error.actual_hex_digest}"
        }
        log_failure(content)
    else:
        LOGGER.info(dumps({"success": True, "message": ""}))
        validation_result_factory.save(item.url, Check.CHECKSUM, ValidationResult.PASSED)

    return 0


if __name__ == "__main__":
    sys.exit(main())
