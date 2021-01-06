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
from jsonschema import Draft7Validator, FormatChecker, RefResolver, ValidationError
from jsonschema._utils import URIDict

S3_URL_PREFIX = "s3://"

SCRIPT_DIR = dirname(__file__)
COLLECTION_SCHEMA_PATH = join(SCRIPT_DIR, "stac-spec/collection-spec/json-schema/collection.json")
CATALOG_SCHEMA_PATH = join(SCRIPT_DIR, "stac-spec/catalog-spec/json-schema/catalog.json")


class STACSchemaValidator:  # pylint:disable=too-few-public-methods
    def __init__(self, url_reader: Callable[[str], TextIO]):
        self.url_reader = url_reader
        self.traversed_urls = []

        with open(COLLECTION_SCHEMA_PATH) as collection_schema_file:
            collection_schema = load(collection_schema_file)

        with open(CATALOG_SCHEMA_PATH) as catalog_schema_file:
            catalog_schema = load(catalog_schema_file)

        # Normalize URLs the same way as jsonschema does
        uri_dictionary = URIDict()
        schema_store = {
            uri_dictionary.normalize(collection_schema["$id"]): collection_schema,
            uri_dictionary.normalize(catalog_schema["$id"]): catalog_schema,
        }

        resolver = RefResolver.from_schema(collection_schema, store=schema_store)
        self.validator = Draft7Validator(
            collection_schema, resolver=resolver, format_checker=FormatChecker()
        )

    def validate(self, url: str) -> None:
        assert url[:5] == S3_URL_PREFIX, f"URL doesn't start with “{S3_URL_PREFIX}”: “{url}”"

        self.traversed_urls.append(url)

        url_stream = self.url_reader(url)
        url_json = load(url_stream)

        self.validator.validate(url_json)

        for link_object in url_json["links"]:
            next_url = link_object["href"]
            if next_url not in self.traversed_urls:
                url_prefix = url.rsplit("/", maxsplit=1)[0]
                next_url_prefix = next_url.rsplit("/", maxsplit=1)[0]
                assert (
                    url_prefix == next_url_prefix
                ), f"“{url}” links to metadata file in different directory: “{next_url}”"

                self.validate(next_url)


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
        STACSchemaValidator(url_reader).validate(arguments.metadata_url)
        print(dumps({"success": True, "message": ""}))
    except (AssertionError, ValidationError) as error:
        print(dumps({"success": False, "message": str(error)}))

    return 0


if __name__ == "__main__":
    sys.exit(main())
