#!/usr/bin/env python3
import logging
import sys
from argparse import ArgumentParser
from json import dumps, load
from os import environ
from os.path import dirname, join
from typing import Callable, TextIO
from urllib.parse import urlparse

import boto3
from botocore.response import StreamingBody
from jsonschema import FormatChecker, ValidationError, validate

SCHEMA_PATH = join(dirname(__file__), "stac-spec/collection-spec/json-schema/collection.json")


def validate_url(url: str, url_reader: Callable[[str], TextIO]) -> None:
    url_stream = url_reader(url)
    url_json = load(url_stream)

    with open(SCHEMA_PATH) as schema_file:
        schema_json = load(schema_file)

    validate(url_json, schema_json, format_checker=FormatChecker())


def parse_arguments():
    argument_parser = ArgumentParser()
    argument_parser.add_argument("--metadata-url", required=True)
    arguments = argument_parser.parse_args()
    return arguments


def s3_url_reader() -> Callable[[str], StreamingBody]:
    client = boto3.client("s3")

    def read(href: str) -> StreamingBody:
        parse_result = urlparse(href, allow_fragments=False)
        bucket_name = parse_result.netloc
        key = parse_result.path[1:]
        response = client.get_object(Bucket=bucket_name, Key=key)
        return response["Body"]

    return read


def set_up_logging():
    logger = logging.getLogger(__name__)

    log_handler = logging.StreamHandler()
    log_level = environ.get("LOGLEVEL", logging.NOTSET)

    logger.addHandler(log_handler)
    logger.setLevel(log_level)

    return logger


def main() -> int:
    logger = set_up_logging()

    arguments = parse_arguments()
    logger.debug(arguments)

    url_reader = s3_url_reader()

    try:
        validate_url(arguments.metadata_url, url_reader)
    except ValidationError as error:
        print(dumps({"success": False, "message": error.message}))

    return 0


if __name__ == "__main__":
    sys.exit(main())
