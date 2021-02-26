#!/usr/bin/env python3
import logging
import sys
from argparse import ArgumentParser, Namespace
from json import dumps, load
from os import environ
from os.path import dirname, join
from typing import Callable, Dict, List
from urllib.parse import urlparse

import boto3
from botocore.response import StreamingBody  # type: ignore[import]
from jsonschema import (  # type: ignore[import]
    Draft7Validator,
    FormatChecker,
    RefResolver,
    ValidationError,
)
from jsonschema._utils import URIDict  # type: ignore[import]

from backend.processing.model import ProcessingAssetsModel

S3_URL_PREFIX = "s3://"

SCRIPT_DIR = dirname(__file__)
COLLECTION_SCHEMA_PATH = join(SCRIPT_DIR, "stac-spec/collection-spec/json-schema/collection.json")
CATALOG_SCHEMA_PATH = join(SCRIPT_DIR, "stac-spec/catalog-spec/json-schema/catalog.json")


class STACSchemaValidator:  # pylint:disable=too-few-public-methods
    def __init__(self, url_reader: Callable[[str], StreamingBody]):
        self.url_reader = url_reader
        self.traversed_urls: List[str] = []

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

    def validate(self, url: str, logger: logging.Logger) -> List[Dict[str, str]]:
        assert url[:5] == S3_URL_PREFIX, f"URL doesn't start with “{S3_URL_PREFIX}”: “{url}”"

        self.traversed_urls.append(url)

        url_stream = self.url_reader(url)
        url_json = load(url_stream)

        self.validator.validate(url_json)

        url_prefix = get_url_before_filename(url)

        assets = []
        for asset in url_json.get("item_assets", {}).values():
            asset_url = asset["href"]
            asset_url_prefix = get_url_before_filename(asset_url)
            assert (
                url_prefix == asset_url_prefix
            ), f"“{url}” links to asset file in different directory: “{asset_url}”"
            asset_dict = {"url": asset_url, "multihash": asset["checksum:multihash"]}
            logger.debug(dumps({"asset": asset_dict}))
            assets.append(asset_dict)

        for link_object in url_json["links"]:
            next_url = link_object["href"]
            if next_url not in self.traversed_urls:
                next_url_prefix = get_url_before_filename(next_url)
                assert (
                    url_prefix == next_url_prefix
                ), f"“{url}” links to metadata file in different directory: “{next_url}”"

                assets.extend(self.validate(next_url, logger))

        return assets


def get_url_before_filename(url: str) -> str:
    return url.rsplit("/", maxsplit=1)[0]


def parse_arguments() -> Namespace:
    argument_parser = ArgumentParser()
    argument_parser.add_argument("--metadata-url", required=True)
    argument_parser.add_argument("--dataset-id", required=True)
    argument_parser.add_argument("--version-id", required=True)
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


def set_up_logging() -> logging.Logger:
    logger = logging.getLogger(__name__)

    log_handler = logging.StreamHandler()
    log_level = environ.get("LOGLEVEL", logging.NOTSET)

    logger.addHandler(log_handler)
    logger.setLevel(log_level)

    return logger


def main() -> int:
    logger = set_up_logging()

    arguments = parse_arguments()
    logger.debug(dumps({"arguments": vars(arguments)}))

    url_reader = s3_url_reader()

    try:
        assets = STACSchemaValidator(url_reader).validate(arguments.metadata_url, logger)
    except (AssertionError, ValidationError) as error:
        logger.error(dumps({"success": False, "message": str(error)}))
        return 1

    asset_pk = f"DATASET#{arguments.dataset_id}#VERSION#{arguments.version_id}"
    for index, asset in enumerate(assets):
        ProcessingAssetsModel(
            pk=asset_pk,
            sk=f"DATA_ITEM_INDEX#{index}",
            url=asset["url"],
            multihash=asset["multihash"],
        ).save()

    logger.info(dumps({"success": True, "message": ""}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
