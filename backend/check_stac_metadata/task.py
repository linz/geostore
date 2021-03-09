#!/usr/bin/env python3
import sys
from argparse import ArgumentParser, Namespace
from json import dumps, load
from logging import Logger
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

from ..log import set_up_logging
from ..processing_assets_model import ProcessingAssetsModel

S3_URL_PREFIX = "s3://"

SCRIPT_DIR = dirname(__file__)
COLLECTION_SCHEMA_PATH = join(SCRIPT_DIR, "stac-spec/collection-spec/json-schema/collection.json")
CATALOG_SCHEMA_PATH = join(SCRIPT_DIR, "stac-spec/catalog-spec/json-schema/catalog.json")


class STACSchemaValidator(Draft7Validator):
    def __init__(self) -> None:
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

        super().__init__(collection_schema, resolver=resolver, format_checker=FormatChecker())


class STACDatasetValidator:
    def __init__(self, url_reader: Callable[[str], StreamingBody], logger: Logger):
        self.url_reader = url_reader
        self.logger = logger

        self.traversed_urls: List[str] = []
        self.dataset_assets: List[Dict[str, str]] = []
        self.dataset_metadata: List[Dict[str, str]] = []

        self.validator = STACSchemaValidator()

    def validate(self, url: str) -> None:
        assert url[:5] == S3_URL_PREFIX, f"URL doesn't start with “{S3_URL_PREFIX}”: “{url}”"

        self.traversed_urls.append(url)

        url_stream = self.url_reader(url)
        url_json = load(url_stream)

        self.validator.validate(url_json)

        url_prefix = get_url_before_filename(url)

        self.dataset_metadata.append({"url": url})

        for asset in url_json.get("item_assets", {}).values():
            asset_url = asset["href"]
            asset_url_prefix = get_url_before_filename(asset_url)
            assert (
                url_prefix == asset_url_prefix
            ), f"“{url}” links to asset file in different directory: “{asset_url}”"
            asset_dict = {"url": asset_url, "multihash": asset["checksum:multihash"]}
            self.logger.debug(dumps({"asset": asset_dict}))
            self.dataset_assets.append(asset_dict)

        for link_object in url_json["links"]:
            next_url = link_object["href"]
            if next_url not in self.traversed_urls:
                next_url_prefix = get_url_before_filename(next_url)
                assert (
                    url_prefix == next_url_prefix
                ), f"“{url}” links to metadata file in different directory: “{next_url}”"
                self.validate(next_url)

    def save(self, key: str) -> None:
        for index, metadata_file in enumerate(self.dataset_metadata):
            ProcessingAssetsModel(
                pk=key,
                sk=f"METADATA_ITEM_INDEX#{index}",
                **metadata_file,
            ).save()

        for index, asset in enumerate(self.dataset_assets):
            ProcessingAssetsModel(
                pk=key,
                sk=f"DATA_ITEM_INDEX#{index}",
                **asset,
            ).save()


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


def main() -> int:
    logger = set_up_logging(__name__)

    arguments = parse_arguments()
    logger.debug(dumps({"arguments": vars(arguments)}))

    url_reader = s3_url_reader()
    validator = STACDatasetValidator(url_reader, logger)

    try:
        validator.validate(arguments.metadata_url)

    except (AssertionError, ValidationError) as error:
        logger.error(dumps({"success": False, "message": str(error)}))
        return 1

    validator.save(f"DATASET#{arguments.dataset_id}#VERSION#{arguments.version_id}")

    logger.info(dumps({"success": True, "message": ""}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
