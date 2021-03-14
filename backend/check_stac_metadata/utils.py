from argparse import ArgumentParser, Namespace
from json import dumps, load
from logging import Logger
from os.path import dirname, join
from typing import Callable, Dict, List, Optional

from botocore.response import StreamingBody  # type: ignore[import]
from jsonschema import (  # type: ignore[import]
    Draft7Validator,
    FormatChecker,
    RefResolver,
    ValidationError,
)
from jsonschema._utils import URIDict  # type: ignore[import]

from ..check import Check
from ..processing_assets_model import ProcessingAssetsModel
from ..types import JsonObject
from ..validation_results_model import ValidationResult, ValidationResultsModel


class STACSchemaValidator(Draft7Validator):
    def __init__(self) -> None:
        self.script_dir = dirname(__file__)

        collection_schema = self.get_schema_dict(
            "stac-spec/collection-spec/json-schema/collection.json"
        )

        schema_store = {}
        uri_dictionary = URIDict()
        for schema in [
            self.get_schema_dict("stac-spec/catalog-spec/json-schema/catalog.json"),
            self.get_schema_dict("stac-spec/catalog-spec/json-schema/catalog-core.json"),
            collection_schema,
        ]:
            # Normalize URLs the same way as jsonschema does
            schema_store[uri_dictionary.normalize(schema["$id"])] = schema

        resolver = RefResolver.from_schema(collection_schema, store=schema_store)

        super().__init__(collection_schema, resolver=resolver, format_checker=FormatChecker())

    def get_schema_dict(self, path: str) -> JsonObject:
        with open(join(self.script_dir, path)) as file_pointer:
            schema_dict: JsonObject = load(file_pointer)
            return schema_dict


class ValidationResultFactory:  # pylint:disable=too-few-public-methods
    def __init__(self, hash_key: str):
        self.hash_key = hash_key

    def save(
        self, url: str, result: ValidationResult, details: Optional[JsonObject] = None
    ) -> None:
        ValidationResultsModel(
            pk=self.hash_key,
            sk=f"CHECK#{Check.JSON_SCHEMA.value}#URL#{url}",
            result=result.value,
            details=details,
        ).save()


class STACDatasetValidator:
    def __init__(
        self,
        url_reader: Callable[[str], StreamingBody],
        validation_result_factory: ValidationResultFactory,
        logger: Logger,
    ):
        self.url_reader = url_reader
        self.validation_result_factory = validation_result_factory
        self.logger = logger

        self.traversed_urls: List[str] = []
        self.dataset_assets: List[Dict[str, str]] = []
        self.dataset_metadata: List[Dict[str, str]] = []

        self.validator = STACSchemaValidator()

    def validate(self, url: str) -> None:
        s3_url_prefix = "s3://"
        assert url[:5] == s3_url_prefix, f"URL doesn't start with “{s3_url_prefix}”: “{url}”"

        self.traversed_urls.append(url)

        url_stream = self.url_reader(url)
        url_json = load(url_stream)

        try:
            self.validator.validate(url_json)
        except ValidationError as error:
            self.validation_result_factory.save(
                url, ValidationResult.FAILED, details={"error_message": str(error)}
            )
            raise
        self.validation_result_factory.save(url, ValidationResult.PASSED)

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
