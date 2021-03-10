from argparse import ArgumentParser, Namespace
from json import dumps, load
from logging import Logger
from os.path import dirname, join
from typing import Callable, Dict, List

from botocore.response import StreamingBody  # type: ignore[import]
from jsonschema import (  # type: ignore[import]
    Draft7Validator,
    FormatChecker,
    RefResolver,
    ValidationError,
)
from jsonschema._utils import URIDict  # type: ignore[import]

from ..processing_assets_model import ProcessingAssetsModel
from ..types import JsonObject
from ..validation_results_model import ValidationResultsModel

S3_URL_PREFIX = "s3://"
SCRIPT_DIR = dirname(__file__)
JSON_SCHEMA_VALIDATION_NAME = "JSON schema validation"


def get_schema_dict(path: str) -> JsonObject:
    with open(path) as file_pointer:
        schema_dict: JsonObject = load(file_pointer)
        return schema_dict


class STACSchemaValidator(Draft7Validator):
    def __init__(self) -> None:
        schema_store = {}
        collection_schema = get_schema_dict(
            join(SCRIPT_DIR, "stac-spec/collection-spec/json-schema/collection.json")
        )

        uri_dictionary = URIDict()
        for schema in [
            get_schema_dict(join(SCRIPT_DIR, "stac-spec/catalog-spec/json-schema/catalog.json")),
            get_schema_dict(
                join(SCRIPT_DIR, "stac-spec/catalog-spec/json-schema/catalog-core.json")
            ),
            collection_schema,
        ]:
            # Normalize URLs the same way as jsonschema does
            schema_store[uri_dictionary.normalize(schema["$id"])] = schema

        resolver = RefResolver.from_schema(collection_schema, store=schema_store)

        super().__init__(collection_schema, resolver=resolver, format_checker=FormatChecker())


class ValidationResultFactory:  # pylint:disable=too-few-public-methods
    def __init__(self, hash_key: str):
        self.hash_key = hash_key

    def save(self, url: str, success: bool) -> None:
        ValidationResultsModel(
            pk=self.hash_key, sk=f"CHECK#{JSON_SCHEMA_VALIDATION_NAME}#URL#{url}", success=success
        ).save()


class STACDatasetValidator:
    def __init__(
        self,
        url_reader: Callable[[str], StreamingBody],
        logger: Logger,
        validation_result_factory: ValidationResultFactory,
    ):
        self.url_reader = url_reader
        self.logger = logger
        self.validation_result_factory = validation_result_factory

        self.traversed_urls: List[str] = []
        self.dataset_assets: List[Dict[str, str]] = []
        self.dataset_metadata: List[Dict[str, str]] = []

        self.validator = STACSchemaValidator()

    def validate(self, url: str) -> None:
        assert url[:5] == S3_URL_PREFIX, f"URL doesn't start with “{S3_URL_PREFIX}”: “{url}”"

        self.traversed_urls.append(url)

        url_stream = self.url_reader(url)
        url_json = load(url_stream)

        try:
            self.validator.validate(url_json)
        except ValidationError:
            self.validation_result_factory.save(url, False)
            raise
        self.validation_result_factory.save(url, True)

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


def parse_arguments(logger: Logger) -> Namespace:
    argument_parser = ArgumentParser()
    argument_parser.add_argument("--metadata-url", required=True)
    argument_parser.add_argument("--dataset-id", required=True)
    argument_parser.add_argument("--version-id", required=True)
    arguments = argument_parser.parse_args()
    logger.debug(dumps({"arguments": vars(arguments)}))
    return arguments
