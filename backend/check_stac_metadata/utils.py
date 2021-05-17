from functools import lru_cache
from json import JSONDecodeError, dumps, load
from os.path import dirname
from typing import Any, Callable, Dict, List, Tuple, Type, Union

from botocore.exceptions import ClientError  # type: ignore[import]
from botocore.response import StreamingBody  # type: ignore[import]
from jsonschema import ValidationError  # type: ignore[import]

from ..api_keys import MESSAGE_KEY, SUCCESS_KEY
from ..check import Check
from ..log import set_up_logging
from ..models import DB_KEY_SEPARATOR
from ..processing_assets_model import ProcessingAssetType, processing_assets_model_with_meta
from ..stac_format import (
    STAC_ASSETS_KEY,
    STAC_CATALOG_TYPE,
    STAC_COLLECTION_TYPE,
    STAC_FILE_CHECKSUM_KEY,
    STAC_HREF_KEY,
    STAC_ITEM_TYPE,
    STAC_LINKS_KEY,
    STAC_TYPE_KEY,
)
from ..types import JsonObject
from ..validation_results_model import ValidationResult, ValidationResultFactory
from .stac_validators import (
    STACCatalogSchemaValidator,
    STACCollectionSchemaValidator,
    STACItemSchemaValidator,
)

LOGGER = set_up_logging(__name__)

STAC_TYPE_VALIDATION_MAP: Dict[
    str,
    Union[
        Type[STACCatalogSchemaValidator],
        Type[STACCollectionSchemaValidator],
        Type[STACItemSchemaValidator],
    ],
] = {
    STAC_COLLECTION_TYPE: STACCollectionSchemaValidator,
    STAC_CATALOG_TYPE: STACCatalogSchemaValidator,
    STAC_ITEM_TYPE: STACItemSchemaValidator,
}

S3_URL_PREFIX = "s3://"

PROCESSING_ASSET_ASSET_KEY = "asset"
PROCESSING_ASSET_MULTIHASH_KEY = "multihash"
PROCESSING_ASSET_URL_KEY = "url"


@lru_cache
def maybe_convert_relative_url_to_absolute(url_or_path: str, parent_url: str) -> str:
    if url_or_path[:5] == S3_URL_PREFIX:
        return url_or_path

    return f"{dirname(parent_url)}/{url_or_path}"


class STACDatasetValidator:
    def __init__(
        self,
        url_reader: Callable[[str], StreamingBody],
        validation_result_factory: ValidationResultFactory,
    ):
        self.url_reader = url_reader
        self.validation_result_factory = validation_result_factory

        self.traversed_urls: List[str] = []
        self.dataset_assets: List[Dict[str, str]] = []
        self.dataset_metadata: List[Dict[str, str]] = []

        self.processing_assets_model = processing_assets_model_with_meta()

    def run(self, metadata_url: str, hash_key: str) -> None:
        if metadata_url[:5] != S3_URL_PREFIX:
            error_message = f"URL doesn't start with “{S3_URL_PREFIX}”: “{metadata_url}”"
            self.validation_result_factory.save(
                metadata_url,
                Check.NON_S3_URL,
                ValidationResult.FAILED,
                details={MESSAGE_KEY: error_message},
            )
            LOGGER.error(dumps({SUCCESS_KEY: False, MESSAGE_KEY: error_message}))
            return

        try:
            self.validate(metadata_url)
        except (ValidationError, ClientError, JSONDecodeError) as error:
            LOGGER.error(dumps({SUCCESS_KEY: False, MESSAGE_KEY: str(error)}))
            return

        for index, metadata_file in enumerate(self.dataset_metadata):
            self.processing_assets_model(
                hash_key=hash_key,
                range_key=f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}{index}",
                url=metadata_file[PROCESSING_ASSET_URL_KEY],
            ).save()

        for index, asset in enumerate(self.dataset_assets):
            self.processing_assets_model(
                hash_key=hash_key,
                range_key=f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}{index}",
                url=asset[PROCESSING_ASSET_URL_KEY],
                multihash=asset[PROCESSING_ASSET_MULTIHASH_KEY],
            ).save()

    def validate(self, url: str) -> None:  # pylint: disable=too-complex
        self.traversed_urls.append(url)
        object_json = self.get_object(url)

        stac_type = object_json[STAC_TYPE_KEY]
        validator = STAC_TYPE_VALIDATION_MAP[stac_type]()

        try:
            validator.validate(object_json)
        except ValidationError as error:
            self.validation_result_factory.save(
                url,
                Check.JSON_SCHEMA,
                ValidationResult.FAILED,
                details={MESSAGE_KEY: str(error)},
            )
            raise
        self.validation_result_factory.save(url, Check.JSON_SCHEMA, ValidationResult.PASSED)
        self.dataset_metadata.append({PROCESSING_ASSET_URL_KEY: url})

        for asset in object_json.get(STAC_ASSETS_KEY, {}).values():
            asset_url = maybe_convert_relative_url_to_absolute(asset[STAC_HREF_KEY], url)

            asset_dict = {
                PROCESSING_ASSET_URL_KEY: asset_url,
                PROCESSING_ASSET_MULTIHASH_KEY: asset[STAC_FILE_CHECKSUM_KEY],
            }
            LOGGER.debug(dumps({PROCESSING_ASSET_ASSET_KEY: asset_dict}))
            self.dataset_assets.append(asset_dict)

        for link_object in object_json[STAC_LINKS_KEY]:
            next_url = maybe_convert_relative_url_to_absolute(link_object[STAC_HREF_KEY], url)

            if next_url not in self.traversed_urls:
                self.validate(next_url)

    def get_object(self, url: str) -> JsonObject:
        try:
            url_stream = self.url_reader(url)
        except ClientError as error:
            self.validation_result_factory.save(
                url,
                Check.STAGING_ACCESS,
                ValidationResult.FAILED,
                details={MESSAGE_KEY: str(error)},
            )
            raise
        try:
            json_object: JsonObject = load(
                url_stream, object_pairs_hook=self.duplicate_object_names_report_builder(url)
            )
        except JSONDecodeError as error:
            self.validation_result_factory.save(
                url, Check.JSON_PARSE, ValidationResult.FAILED, details={MESSAGE_KEY: str(error)}
            )
            raise
        return json_object

    def duplicate_object_names_report_builder(
        self, url: str
    ) -> Callable[[List[Tuple[str, Any]]], JsonObject]:
        def report_duplicate_object_names(object_pairs: List[Tuple[str, Any]]) -> JsonObject:
            result = {}
            for key, value in object_pairs:
                if key in result:
                    self.validation_result_factory.save(
                        url,
                        Check.DUPLICATE_OBJECT_KEY,
                        ValidationResult.FAILED,
                        details={MESSAGE_KEY: f"Found duplicate object name “{key}” in “{url}”"},
                    )
                else:
                    result[key] = value
            return result

        return report_duplicate_object_names


@lru_cache
def get_url_before_filename(url: str) -> str:
    return url.rsplit("/", maxsplit=1)[0]
