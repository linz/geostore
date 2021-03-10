#!/usr/bin/env python3
import sys
from json import dumps
from typing import Callable
from urllib.parse import urlparse

import boto3
from botocore.response import StreamingBody  # type: ignore[import]
from jsonschema import ValidationError  # type: ignore[import]

from ..log import set_up_logging
from .utils import STACDatasetValidator, ValidationResultFactory, parse_arguments


def s3_url_reader() -> Callable[[str], StreamingBody]:
    client = boto3.client("s3")

    def read(href: str) -> StreamingBody:
        parse_result = urlparse(href, allow_fragments=False)
        bucket_name = parse_result.netloc
        key = parse_result.path[1:]
        response = client.get_object(Bucket=bucket_name, Key=key)
        return response["Body"]

    return read


def main() -> int:
    logger = set_up_logging(__name__)

    arguments = parse_arguments(logger)

    url_reader = s3_url_reader()
    hash_key = f"DATASET#{arguments.dataset_id}#VERSION#{arguments.version_id}"
    validation_result_factory = ValidationResultFactory(hash_key)
    validator = STACDatasetValidator(url_reader, logger, validation_result_factory)

    try:
        validator.validate(arguments.metadata_url)

    except (AssertionError, ValidationError) as error:
        logger.error(dumps({"success": False, "message": str(error)}))
        return 1

    validator.save(hash_key)

    logger.info(dumps({"success": True, "message": ""}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
