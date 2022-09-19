from functools import lru_cache
from json import JSONDecodeError, load
from logging import Logger
from os.path import basename, dirname
from typing import Any, Callable, Dict, List, Tuple, Union

from botocore.exceptions import ClientError
from jsonschema import Draft7Validator, ValidationError
from linz_logger import get_log

from ..api_keys import MESSAGE_KEY
from ..check import Check
from ..logging_keys import GIT_COMMIT, LOG_MESSAGE_VALIDATION_COMPLETE
from ..models import DB_KEY_SEPARATOR
from ..parameter_store import ParameterName, get_param
from ..processing_assets_model import ProcessingAssetType, processing_assets_model_with_meta
from ..s3 import S3_URL_PREFIX
from ..s3_utils import GeostoreS3Response
from ..stac_format import (
    LINZ_STAC_SECURITY_CLASSIFICATION_KEY,
    LINZ_STAC_SECURITY_CLASSIFICATION_UNCLASSIFIED,
    STAC_ASSETS_KEY,
    STAC_FILE_CHECKSUM_KEY,
    STAC_HREF_KEY,
    STAC_LINKS_KEY,
    STAC_REL_CHILD,
    STAC_REL_ITEM,
    STAC_REL_KEY,
    STAC_TYPE_CATALOG,
    STAC_TYPE_COLLECTION,
    STAC_TYPE_ITEM,
    STAC_TYPE_KEY,
)
from ..step_function import AssetGarbageCollector, Outcome
from ..types import JsonObject
from ..validation_results_model import ValidationResult, ValidationResultFactory
from .stac_validators import (
    STACCatalogSchemaValidator,
    STACCollectionSchemaValidator,
    STACItemSchemaValidator,
)

LOGGER: Logger = get_log()

STAC_TYPE_VALIDATION_MAP: Dict[str, Draft7Validator] = {
    STAC_TYPE_CATALOG: STACCatalogSchemaValidator,
    STAC_TYPE_COLLECTION: STACCollectionSchemaValidator,
    STAC_TYPE_ITEM: STACItemSchemaValidator,
}

PROCESSING_ASSET_ASSET_KEY = "asset"
PROCESSING_ASSET_MULTIHASH_KEY = "multihash"
PROCESSING_ASSET_URL_KEY = "url"
PROCESSING_ASSET_FILE_IN_STAGING_KEY = "file_in_staging"
EXPLICITLY_RELATIVE_PATH_PREFIX = "./"
LOG_MESSAGE_STAC_ASSET_INFO = "STACAsset:Info"


@lru_cache
def maybe_convert_relative_url_to_absolute(url_or_path: str, parent_url: str) -> str:
    if url_or_path.startswith(S3_URL_PREFIX):
        return url_or_path

    if url_or_path.startswith(EXPLICITLY_RELATIVE_PATH_PREFIX):
        url_or_path = url_or_path[len(EXPLICITLY_RELATIVE_PATH_PREFIX) :]

    return f"{dirname(parent_url)}/{url_or_path}"


def is_url_start_with_s3(metadata_url: str) -> bool:
    return metadata_url[:5] == S3_URL_PREFIX


def is_instance_of_catalog_or_collection(stac_type: str) -> bool:
    return stac_type in (STAC_TYPE_COLLECTION, STAC_TYPE_CATALOG)


class STACDatasetValidator:
    # pylint:disable=too-many-instance-attributes
    def __init__(
        self,
        hash_key: str,
        url_reader: Callable[[str], GeostoreS3Response],
        asset_garbage_collector: AssetGarbageCollector,
        validation_result_factory: ValidationResultFactory,
    ):
        self.hash_key = hash_key
        self.url_reader = url_reader
        self.asset_garbage_collector = asset_garbage_collector
        self.validation_result_factory = validation_result_factory

        self.traversed_urls: List[str] = []
        self.dataset_assets: List[Dict[str, str]] = []
        self.dataset_metadata: List[Dict[str, Union[bool, str]]] = []

        self.processing_assets_model = processing_assets_model_with_meta()

    def run(self, metadata_url: str) -> None:
        if not is_url_start_with_s3(metadata_url):
            error_message = f"URL doesn't start with “{S3_URL_PREFIX}”: “{metadata_url}”"
            self.validation_result_factory.save(
                metadata_url,
                Check.NON_S3_URL,
                ValidationResult.FAILED,
                details={MESSAGE_KEY: error_message},
            )
            LOGGER.error(
                LOG_MESSAGE_VALIDATION_COMPLETE,
                extra={
                    "outcome": Outcome.FAILED,
                    "error": error_message,
                    GIT_COMMIT: get_param(ParameterName.GIT_COMMIT),
                },
            )
            raise InvalidAssetFileError()
        try:
            self.validate(metadata_url)
        except (
            ClientError,
            InvalidSecurityClassificationError,
            JSONDecodeError,
            ValidationError,
        ) as error:
            LOGGER.error(
                LOG_MESSAGE_VALIDATION_COMPLETE,
                extra={
                    "outcome": Outcome.FAILED,
                    "error": str(error),
                    GIT_COMMIT: get_param(ParameterName.GIT_COMMIT),
                },
            )
            return

        if not self.dataset_assets:
            error_details = {MESSAGE_KEY: Check.NO_ASSETS_FOUND_ERROR_MESSAGE}
            self.validation_result_factory.save(
                metadata_url,
                Check.ASSETS_IN_DATASET,
                ValidationResult.FAILED,
                details=error_details,
            )
            LOGGER.error(
                LOG_MESSAGE_VALIDATION_COMPLETE,
                extra={
                    "outcome": Outcome.FAILED,
                    "error": Check.NO_ASSETS_FOUND_ERROR_MESSAGE,
                    GIT_COMMIT: get_param(ParameterName.GIT_COMMIT),
                },
            )
            return

        s3_response = self.get_object(metadata_url)
        stac_type = self.get_s3_url_as_object_json(metadata_url, s3_response)[STAC_TYPE_KEY]
        if not is_instance_of_catalog_or_collection(stac_type):
            error_message = (
                f"Uploaded Assets should be catalog.json or collection.json”: “{metadata_url}”"
            )
            self.validation_result_factory.save(
                metadata_url,
                Check.UPLOADED_ASSETS_SHOULD_BE_CATALOG_OR_COLLECTION_ERROR,
                ValidationResult.FAILED,
                details={MESSAGE_KEY: error_message},
            )
            LOGGER.error(
                LOG_MESSAGE_VALIDATION_COMPLETE,
                extra={
                    "outcome": Outcome.FAILED,
                    "error": Check.UPLOADED_ASSETS_SHOULD_BE_CATALOG_OR_COLLECTION_ERROR,
                    GIT_COMMIT: get_param(ParameterName.GIT_COMMIT),
                },
            )
            raise InvalidAssetFileError()

        self.process_metadata()
        self.process_assets()

    def process_metadata(self) -> None:
        for index, metadata_file in enumerate(self.dataset_metadata):
            asset_url = metadata_file[PROCESSING_ASSET_URL_KEY]
            assert isinstance(asset_url, str)

            self.processing_assets_model(
                hash_key=self.hash_key,
                range_key=f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}{index}",
                url=asset_url,
                filename=basename(asset_url),
                exists_in_staging=metadata_file[PROCESSING_ASSET_FILE_IN_STAGING_KEY],
            ).save()

    def process_assets(self) -> None:
        for index, asset in enumerate(self.dataset_assets):
            asset_url = asset[PROCESSING_ASSET_URL_KEY]
            assert isinstance(asset_url, str)

            self.processing_assets_model(
                hash_key=self.hash_key,
                range_key=f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}{index}",
                url=asset_url,
                filename=basename(asset_url),
                multihash=asset[PROCESSING_ASSET_MULTIHASH_KEY],
            ).save()

    def validate(self, url: str) -> None:  # pylint: disable=too-complex
        self.traversed_urls.append(url)
        s3_response = self.get_object(url)
        object_json = self.get_s3_url_as_object_json(url, s3_response)

        stac_type = object_json[STAC_TYPE_KEY]
        validator = STAC_TYPE_VALIDATION_MAP[stac_type]

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

        security_classification = object_json.get(LINZ_STAC_SECURITY_CLASSIFICATION_KEY)
        if (
            security_classification is not None
            and security_classification != LINZ_STAC_SECURITY_CLASSIFICATION_UNCLASSIFIED
        ):
            self.validation_result_factory.save(
                url,
                Check.SECURITY_CLASSIFICATION,
                ValidationResult.FAILED,
                details={
                    MESSAGE_KEY: "Expected security classification of "
                    f"'{LINZ_STAC_SECURITY_CLASSIFICATION_UNCLASSIFIED}'. "
                    f"Got '{security_classification}'."
                },
            )
            raise InvalidSecurityClassificationError(security_classification)

        self.validation_result_factory.save(url, Check.JSON_SCHEMA, ValidationResult.PASSED)

        self.dataset_metadata.append(
            {
                PROCESSING_ASSET_URL_KEY: url,
                PROCESSING_ASSET_FILE_IN_STAGING_KEY: s3_response.file_in_staging,
            }
        )

        self.asset_garbage_collector.mark_asset_as_replaced(basename(url))

        for asset in object_json.get(STAC_ASSETS_KEY, {}).values():
            asset_url = maybe_convert_relative_url_to_absolute(asset[STAC_HREF_KEY], url)

            asset_dict = {
                PROCESSING_ASSET_URL_KEY: asset_url,
                PROCESSING_ASSET_MULTIHASH_KEY: asset[STAC_FILE_CHECKSUM_KEY],
            }
            LOGGER.debug(
                LOG_MESSAGE_STAC_ASSET_INFO,
                extra={"asset": asset_dict, GIT_COMMIT: get_param(ParameterName.GIT_COMMIT)},
            )
            self.dataset_assets.append(asset_dict)

        for link_object in object_json[STAC_LINKS_KEY]:
            if link_object[STAC_REL_KEY] not in [STAC_REL_CHILD, STAC_REL_ITEM]:
                continue

            next_url = maybe_convert_relative_url_to_absolute(link_object[STAC_HREF_KEY], url)

            if next_url not in self.traversed_urls:
                self.validate(next_url)

    def get_s3_url_as_object_json(self, url: str, s3_response: GeostoreS3Response) -> JsonObject:
        try:
            object_json: JsonObject = load(
                s3_response.response,
                object_pairs_hook=self.duplicate_object_names_report_builder(url),
            )
        except JSONDecodeError as error:
            self.validation_result_factory.save(
                url, Check.JSON_PARSE, ValidationResult.FAILED, details={MESSAGE_KEY: str(error)}
            )
            raise
        return object_json

    def get_object(self, url: str) -> GeostoreS3Response:
        try:
            s3_response = self.url_reader(url)
        except ClientError as error:
            if error.response["Error"]["Code"] == "NoSuchKey":
                self.validation_result_factory.save(
                    url,
                    Check.FILE_NOT_FOUND,
                    ValidationResult.FAILED,
                    details={
                        MESSAGE_KEY: f"Could not find metadata file '{url}' "
                        f"in staging bucket or in the Geostore."
                    },
                )
                raise
            self.validation_result_factory.save(
                url,
                Check.STAGING_ACCESS,
                ValidationResult.FAILED,
                details={MESSAGE_KEY: str(error)},
            )
            raise
        return s3_response

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


class InvalidSecurityClassificationError(Exception):
    pass


class InvalidAssetFileError(Exception):
    pass
