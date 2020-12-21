#!/usr/bin/env python3
import logging
import sys
from argparse import ArgumentParser
from json import dumps, load
from os import environ
from os.path import dirname, join
from typing import Callable, Iterable, Optional, TextIO
from urllib.parse import urlparse

import boto3
from botocore.response import StreamingBody
from jsonschema import Draft7Validator, FormatChecker, RefResolver, ValidationError

SCRIPT_DIR = dirname(__file__)
COLLECTION_SCHEMA_PATH = join(SCRIPT_DIR, "stac-spec/collection-spec/json-schema/collection.json")
CATALOG_SCHEMA_PATH = join(SCRIPT_DIR, "stac-spec/catalog-spec/json-schema/catalog.json")


def validate_url(
    url: str, url_reader: Callable[[str], TextIO], traversed_urls: Optional[Iterable[str]] = None
) -> None:
    if traversed_urls is None:
        traversed_urls = []
    traversed_urls.append(url)

    url_stream = url_reader(url)
    url_json = load(url_stream)

    with open(COLLECTION_SCHEMA_PATH) as collection_schema_file:
        collection_schema = load(collection_schema_file)

    with open(CATALOG_SCHEMA_PATH) as catalog_schema_file:
        catalog_schema = load(catalog_schema_file)

    schema_store = {
        collection_schema["$id"]: collection_schema,
        catalog_schema["$id"]: catalog_schema,
    }

    resolver = RefResolver.from_schema(collection_schema, store=schema_store)
    validator = Draft7Validator(
        collection_schema, resolver=resolver, format_checker=FormatChecker()
    )
    validator.validate(url_json)

    for link_object in url_json["links"]:
        next_url = link_object["href"]
        if next_url not in traversed_urls:
            validate_url(next_url, url_reader, traversed_urls)


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
        print(dumps({"success": True, "message": ""}))
    except ValidationError as error:
        print(dumps({"success": False, "message": error.message}))

    return 0


if __name__ == "__main__":
    sys.exit(main())
