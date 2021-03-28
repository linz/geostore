from argparse import ArgumentParser, Namespace
from json import dumps, load
from logging import Logger
from os.path import dirname, join
from typing import Any, Callable, Dict, List

from botocore.exceptions import ClientError  # type: ignore[import]
from botocore.response import StreamingBody  # type: ignore[import]
from jsonschema import (  # type: ignore[import]
    Draft7Validator,
    FormatChecker,
    RefResolver,
    ValidationError,
)
from jsonschema._utils import URIDict  # type: ignore[import]

from ..check import Check
from ..processing_assets_model import ProcessingAssetType, ProcessingAssetsModel
from ..types import JsonObject
from ..validation_results_model import ValidationResult, ValidationResultFactory


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
            self.get_schema_dict("stac-spec/item-spec/json-schema/basics.json"),
            self.get_schema_dict("stac-spec/item-spec/json-schema/datetime.json"),
            self.get_schema_dict("stac-spec/item-spec/json-schema/instrument.json"),
            self.get_schema_dict("stac-spec/item-spec/json-schema/item.json"),
            self.get_schema_dict("stac-spec/item-spec/json-schema/licensing.json"),
            self.get_schema_dict("stac-spec/item-spec/json-schema/provider.json"),
        ]:
            # Normalize URLs the same way as jsonschema does
            schema_store[uri_dictionary.normalize(schema["$id"])] = schema

        resolver = RefResolver.from_schema(collection_schema, store=schema_store)

        super().__init__(collection_schema, resolver=resolver, format_checker=FormatChecker())

    def get_schema_dict(self, path: str) -> JsonObject:
        with open(join(self.script_dir, path)) as file_pointer:
            schema_dict: JsonObject = load(file_pointer)
            return schema_dict


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

    def run(self, metadata_url: str) -> None:
        s3_url_prefix = "s3://"

        if metadata_url[:5] != s3_url_prefix:
            error_message = f"URL doesn't start with “{s3_url_prefix}”: “{metadata_url}”"
            self.validation_result_factory.save(
                metadata_url,
                Check.NON_S3_URL,
                ValidationResult.FAILED,
                details={"message": error_message},
            )
            raise AssertionError(error_message)
        self.validate(metadata_url)

    def validate(self, url: str) -> None:  # pylint: disable=too-complex
        self.traversed_urls.append(url)
        object_json = self.get_object(url)

        try:
            self.validator.validate(object_json)
        except ValidationError as error:
            self.validation_result_factory.save(
                url,
                Check.JSON_SCHEMA,
                ValidationResult.FAILED,
                details={"message": str(error)},
            )
            raise
        self.validation_result_factory.save(url, Check.JSON_SCHEMA, ValidationResult.PASSED)
        self.dataset_metadata.append({"url": url})

        for asset in object_json.get("assets", {}).values():
            asset_url = asset["href"]
            self.validate_directory(asset_url, url)

            asset_dict = {"url": asset_url, "multihash": asset["checksum:multihash"]}
            self.logger.debug(dumps({"asset": asset_dict}))
            self.dataset_assets.append(asset_dict)

        for link_object in object_json["links"]:
            next_url = link_object["href"]
            if next_url not in self.traversed_urls:
                self.validate_directory(next_url, url)
                self.validate(next_url)

    def get_object(self, url: str) -> Any:
        try:
            url_stream = self.url_reader(url)
        except ClientError as error:
            self.validation_result_factory.save(
                url,
                Check.STAGING_ACCESS,
                ValidationResult.FAILED,
                details={"message": str(error)},
            )
            raise
        return load(url_stream)

    def save(self, key: str) -> None:
        for index, metadata_file in enumerate(self.dataset_metadata):
            ProcessingAssetsModel(
                hash_key=key,
                range_key=f"{ProcessingAssetType.METADATA.value}#{index}",
                url=metadata_file["url"],
            ).save()

        for index, asset in enumerate(self.dataset_assets):
            ProcessingAssetsModel(
                hash_key=key,
                range_key=f"{ProcessingAssetType.DATA.value}#{index}",
                url=asset["url"],
                multihash=asset["multihash"],
            ).save()

    def validate_directory(self, url: str, parent_metadata_url: str) -> None:
        root_path = get_url_before_filename(self.traversed_urls[0])
        if root_path != get_url_before_filename(url):
            self.validation_result_factory.save(
                url,
                Check.MULTIPLE_DIRECTORIES,
                ValidationResult.FAILED,
                details={
                    "message": f"“metadata file: {parent_metadata_url} links to {url}”"
                    f" which exists in a different directory to the root "
                    f"metadata file directory: “{root_path}”"
                },
            )


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
