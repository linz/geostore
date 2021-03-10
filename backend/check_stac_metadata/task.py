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

LOGGER = set_up_logging(__name__)
S3_CLIENT = boto3.client("s3")


def s3_url_reader() -> Callable[[str], StreamingBody]:
    def read(href: str) -> StreamingBody:
        parse_result = urlparse(href, allow_fragments=False)
        bucket_name = parse_result.netloc
        key = parse_result.path[1:]
        response = S3_CLIENT.get_object(Bucket=bucket_name, Key=key)
        return response["Body"]

    return read


def main() -> int:
    arguments = parse_arguments(LOGGER)

    url_reader = s3_url_reader()
    hash_key = f"DATASET#{arguments.dataset_id}#VERSION#{arguments.version_id}"
    validation_result_factory = ValidationResultFactory(hash_key)
    validator = STACDatasetValidator(url_reader, LOGGER, validation_result_factory)

    try:
        validator.validate(arguments.metadata_url)

    except (AssertionError, ValidationError) as error:
        LOGGER.error(dumps({"success": False, "message": str(error)}))
        return 1

    validator.save(hash_key)

    LOGGER.info(dumps({"success": True, "message": ""}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
