# pylint: disable=too-many-lines

from copy import deepcopy
from datetime import timedelta
from glob import glob
from hashlib import sha256, sha512
from io import BytesIO
from json import JSONDecodeError, dumps, load
from os.path import basename
from typing import TYPE_CHECKING, Any, Dict, List, Tuple
from unittest.mock import MagicMock, call, patch

from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from jsonschema import ValidationError
from pytest import mark, raises
from pytest_subtests import SubTests

from geostore.api_keys import MESSAGE_KEY, SUCCESS_KEY
from geostore.check import Check
from geostore.check_stac_metadata.stac_validators import (
    LINZ_SCHEMA_URL_DIRECTORY,
    STACCatalogSchemaValidator,
    STACCollectionSchemaValidator,
    STACItemSchemaValidator,
)
from geostore.check_stac_metadata.task import lambda_handler
from geostore.check_stac_metadata.utils import (
    NO_ASSETS_FOUND_ERROR_MESSAGE,
    PROCESSING_ASSET_FILE_IN_STAGING_KEY,
    PROCESSING_ASSET_MULTIHASH_KEY,
    PROCESSING_ASSET_URL_KEY,
    InvalidSecurityClassificationError,
    STACDatasetValidator,
)
from geostore.logging_keys import LOG_MESSAGE_VALIDATION_COMPLETE
from geostore.models import CHECK_ID_PREFIX, DB_KEY_SEPARATOR, URL_ID_PREFIX
from geostore.parameter_store import ParameterName, get_param
from geostore.populate_catalog.task import CATALOG_FILENAME
from geostore.processing_assets_model import ProcessingAssetType, processing_assets_model_with_meta
from geostore.resources import Resource
from geostore.s3 import S3_URL_PREFIX
from geostore.stac_format import (
    LINZ_STAC_CREATED_KEY,
    LINZ_STAC_EXTENSIONS_LOCAL_PATH,
    LINZ_STAC_SECURITY_CLASSIFICATION_KEY,
    LINZ_STAC_SECURITY_CLASSIFICATION_UNCLASSIFIED,
    LINZ_STAC_UPDATED_KEY,
    STAC_ASSETS_KEY,
    STAC_FILE_CHECKSUM_KEY,
    STAC_HREF_KEY,
    STAC_ID_KEY,
    STAC_LINKS_KEY,
    STAC_REL_CHILD,
    STAC_REL_ITEM,
    STAC_REL_KEY,
    STAC_REL_PARENT,
    STAC_REL_ROOT,
    STAC_REL_SELF,
)
from geostore.step_function import Outcome, get_hash_key
from geostore.step_function_keys import (
    DATASET_ID_KEY,
    DATASET_PREFIX_KEY,
    METADATA_URL_KEY,
    S3_ROLE_ARN_KEY,
    VERSION_ID_KEY,
)
from geostore.validation_results_model import ValidationResult, validation_results_model_with_meta

from .aws_utils import (
    MockGeostoreS3Response,
    MockJSONURLReader,
    MockValidationResultFactory,
    S3Object,
    any_lambda_context,
    any_role_arn,
    any_s3_url,
    any_table_name,
    get_s3_role_arn,
)
from .dynamodb_generators import any_hash_key
from .file_utils import json_dict_to_file_object
from .general_generators import (
    any_error_message,
    any_file_contents,
    any_https_url,
    any_past_datetime_string,
    any_safe_file_path,
    any_safe_filename,
)
from .stac_generators import (
    any_asset_name,
    any_dataset_id,
    any_dataset_prefix,
    any_dataset_version_id,
    any_hex_multihash,
)
from .stac_objects import (
    MINIMAL_VALID_STAC_CATALOG_OBJECT,
    MINIMAL_VALID_STAC_COLLECTION_OBJECT,
    MINIMAL_VALID_STAC_ITEM_OBJECT,
)

if TYPE_CHECKING:
    from botocore.exceptions import ClientErrorResponseError, ClientErrorResponseTypeDef
else:
    ClientErrorResponseError = ClientErrorResponseTypeDef = dict


@patch("geostore.check_stac_metadata.task.STACDatasetValidator.validate")
@patch("geostore.check_stac_metadata.task.get_s3_url_reader")
def should_succeed_with_validation_failure(
    get_s3_url_reader_mock: MagicMock, validate_url_mock: MagicMock
) -> None:
    validate_url_mock.side_effect = ValidationError(any_error_message())
    get_s3_url_reader_mock.return_value.return_value = MockGeostoreS3Response(
        MINIMAL_VALID_STAC_COLLECTION_OBJECT, file_in_staging=True
    )

    with patch("geostore.check_stac_metadata.utils.processing_assets_model_with_meta"):
        lambda_handler(
            {
                DATASET_ID_KEY: any_dataset_id(),
                VERSION_ID_KEY: any_dataset_version_id(),
                METADATA_URL_KEY: any_s3_url(),
                S3_ROLE_ARN_KEY: any_role_arn(),
                DATASET_PREFIX_KEY: any_dataset_prefix(),
            },
            any_lambda_context(),
        )


@patch("geostore.check_stac_metadata.task.ValidationResultFactory")
@patch("geostore.check_stac_metadata.task.get_s3_url_reader")
@patch("geostore.check_stac_metadata.task.get_param")
def should_save_non_s3_url_validation_results(
    get_param_mock: MagicMock,
    get_s3_url_reader_mock: MagicMock,
    validation_results_factory_mock: MagicMock,
) -> None:
    # Given
    validation_results_table_name = any_table_name()
    get_param_mock.return_value = validation_results_table_name
    non_s3_url = any_https_url()
    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()
    get_s3_url_reader_mock.return_value.return_value = MockGeostoreS3Response(
        MINIMAL_VALID_STAC_COLLECTION_OBJECT, file_in_staging=True
    )

    with patch("geostore.check_stac_metadata.utils.processing_assets_model_with_meta"):
        # When
        lambda_handler(
            {
                DATASET_ID_KEY: dataset_id,
                VERSION_ID_KEY: version_id,
                METADATA_URL_KEY: non_s3_url,
                S3_ROLE_ARN_KEY: any_role_arn(),
                DATASET_PREFIX_KEY: any_dataset_prefix(),
            },
            any_lambda_context(),
        )

    # Then
    hash_key = get_hash_key(dataset_id, version_id)
    assert validation_results_factory_mock.mock_calls == [
        call(hash_key, validation_results_table_name),
        call().save(
            non_s3_url,
            Check.NON_S3_URL,
            ValidationResult.FAILED,
            details={MESSAGE_KEY: f"URL doesn't start with “{S3_URL_PREFIX}”: “{non_s3_url}”"},
        ),
    ]


@patch("geostore.check_stac_metadata.task.ValidationResultFactory")
def should_report_duplicate_asset_names(validation_results_factory_mock: MagicMock) -> None:
    # Given
    asset_name = "name"

    class TupleArrayDict(Dict[str, Any]):
        def __init__(self, items: List[Tuple[str, Any]]) -> None:
            super().__init__()
            self["dummy"] = "dummy"

            self._items = items

        def items(self) -> List[Tuple[str, Any]]:  # type: ignore[override]
            return self._items

    assets = [
        (
            asset_name,
            {
                STAC_HREF_KEY: f"{S3_URL_PREFIX}bucket/foo",
                STAC_FILE_CHECKSUM_KEY: any_hex_multihash(),
            },
        ),
        (
            asset_name,
            {
                STAC_HREF_KEY: f"{S3_URL_PREFIX}bucket/bar",
                STAC_FILE_CHECKSUM_KEY: any_hex_multihash(),
            },
        ),
    ]
    metadata = dumps(
        TupleArrayDict(list(MINIMAL_VALID_STAC_COLLECTION_OBJECT.items()) + assets)
    ).encode()

    metadata_url = any_s3_url()

    url_reader = MockJSONURLReader(
        {
            metadata_url: MockGeostoreS3Response(
                StreamingBody(BytesIO(metadata), len(metadata)), file_in_staging=True
            )
        }
    )

    with patch("geostore.check_stac_metadata.utils.processing_assets_model_with_meta"):
        # When
        STACDatasetValidator(any_hash_key(), url_reader, validation_results_factory_mock).validate(
            metadata_url
        )

    # Then
    validation_results_factory_mock.save.assert_any_call(
        metadata_url,
        Check.DUPLICATE_OBJECT_KEY,
        ValidationResult.FAILED,
        details={MESSAGE_KEY: f"Found duplicate object name “{asset_name}” in “{metadata_url}”"},
    )


@mark.infrastructure
@patch("geostore.check_stac_metadata.task.get_s3_url_reader")
@patch("geostore.check_stac_metadata.task.ValidationResultFactory")
def should_save_staging_access_validation_results(
    validation_results_factory_mock: MagicMock,
    get_s3_url_reader_mock: MagicMock,
) -> None:
    validation_results_table_name = get_param(ParameterName.STORAGE_VALIDATION_RESULTS_TABLE_NAME)
    expected_error = ClientError(
        ClientErrorResponseTypeDef(Error=ClientErrorResponseError(Code="TEST", Message="TEST")),
        operation_name="get_object",
    )
    get_s3_url_reader_mock.return_value.side_effect = expected_error
    s3_url = any_s3_url()
    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    lambda_handler(
        {
            DATASET_ID_KEY: dataset_id,
            VERSION_ID_KEY: version_id,
            METADATA_URL_KEY: s3_url,
            S3_ROLE_ARN_KEY: any_role_arn(),
            DATASET_PREFIX_KEY: any_dataset_prefix(),
        },
        any_lambda_context(),
    )

    hash_key = get_hash_key(dataset_id, version_id)
    assert validation_results_factory_mock.mock_calls == [
        call(hash_key, validation_results_table_name),
        call().save(
            s3_url,
            Check.STAGING_ACCESS,
            ValidationResult.FAILED,
            details={MESSAGE_KEY: str(expected_error)},
        ),
    ]


@mark.infrastructure
@patch("geostore.check_stac_metadata.task.get_s3_url_reader")
@patch("geostore.check_stac_metadata.task.ValidationResultFactory")
def should_save_file_not_found_validation_results(
    validation_results_factory_mock: MagicMock,
    get_s3_client_for_role_mock: MagicMock,
) -> None:

    validation_results_table_name = get_param(ParameterName.STORAGE_VALIDATION_RESULTS_TABLE_NAME)
    expected_error = ClientError(
        ClientErrorResponseTypeDef(
            Error=ClientErrorResponseError(Code="NoSuchKey", Message="TEST")
        ),
        operation_name="get_object",
    )

    get_s3_client_for_role_mock.return_value.side_effect = expected_error

    s3_url = any_s3_url()

    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    lambda_handler(
        {
            DATASET_ID_KEY: dataset_id,
            VERSION_ID_KEY: version_id,
            METADATA_URL_KEY: s3_url,
            S3_ROLE_ARN_KEY: any_role_arn(),
            DATASET_PREFIX_KEY: any_dataset_prefix(),
        },
        any_lambda_context(),
    )

    hash_key = get_hash_key(dataset_id, version_id)
    assert validation_results_factory_mock.mock_calls == [
        call(hash_key, validation_results_table_name),
        call().save(
            s3_url,
            Check.FILE_NOT_FOUND,
            ValidationResult.FAILED,
            details={
                MESSAGE_KEY: f"Could not find metadata file '{s3_url}' "
                f"in staging bucket or in the Geostore."
            },
        ),
    ]


@mark.infrastructure
def should_save_json_schema_validation_results_per_file(subtests: SubTests) -> None:
    base_url = f"{S3_URL_PREFIX}{Resource.STAGING_BUCKET_NAME.resource_name}/"
    valid_child_key = any_safe_filename()
    invalid_child_key = any_safe_filename()
    invalid_stac_object = deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)
    invalid_stac_object.pop(STAC_ID_KEY)

    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    with S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                STAC_LINKS_KEY: [
                    {STAC_HREF_KEY: f"{base_url}{valid_child_key}", "rel": "child"},
                    {STAC_HREF_KEY: f"{base_url}{invalid_child_key}", "rel": "child"},
                ],
            }
        ),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=any_safe_filename(),
    ) as root_s3_object, S3Object(
        file_object=json_dict_to_file_object(deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=valid_child_key,
    ) as valid_child_s3_object, S3Object(
        file_object=json_dict_to_file_object(invalid_stac_object),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=invalid_child_key,
    ) as invalid_child_s3_object:
        # When
        assert lambda_handler(
            {
                DATASET_ID_KEY: dataset_id,
                VERSION_ID_KEY: version_id,
                METADATA_URL_KEY: root_s3_object.url,
                S3_ROLE_ARN_KEY: get_s3_role_arn(),
                DATASET_PREFIX_KEY: any_dataset_prefix(),
            },
            any_lambda_context(),
        ) == {SUCCESS_KEY: True}

    hash_key = get_hash_key(dataset_id, version_id)
    validation_results_model = validation_results_model_with_meta()
    with subtests.test(msg="Root validation results"):
        assert (
            validation_results_model.get(
                hash_key=hash_key,
                range_key=(
                    f"{CHECK_ID_PREFIX}{Check.JSON_SCHEMA.value}"
                    f"{DB_KEY_SEPARATOR}{URL_ID_PREFIX}{root_s3_object.url}"
                ),
                consistent_read=True,
            ).result
            == ValidationResult.PASSED.value
        )

    with subtests.test(msg="Valid child validation results"):
        assert (
            validation_results_model.get(
                hash_key=hash_key,
                range_key=(
                    f"{CHECK_ID_PREFIX}{Check.JSON_SCHEMA.value}"
                    f"{DB_KEY_SEPARATOR}{URL_ID_PREFIX}{valid_child_s3_object.url}"
                ),
                consistent_read=True,
            ).result
            == ValidationResult.PASSED.value
        )

    with subtests.test(msg="Invalid child validation results"):
        assert (
            validation_results_model.get(
                hash_key=hash_key,
                range_key=(
                    f"{CHECK_ID_PREFIX}{Check.JSON_SCHEMA.value}"
                    f"{DB_KEY_SEPARATOR}{URL_ID_PREFIX}{invalid_child_s3_object.url}"
                ),
                consistent_read=True,
            ).result
            == ValidationResult.FAILED.value
        )


@mark.timeout(timedelta(minutes=20).total_seconds())
@mark.infrastructure
def should_insert_asset_urls_and_checksums_into_database(subtests: SubTests) -> None:
    # pylint: disable=too-many-locals
    # Given a metadata file with two assets
    first_asset_content = any_file_contents()
    first_asset_multihash = sha256(first_asset_content).hexdigest()

    second_asset_content = any_file_contents()
    second_asset_multihash = sha512(second_asset_content).hexdigest()

    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    with S3Object(
        file_object=BytesIO(initial_bytes=first_asset_content),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=any_safe_filename(),
    ) as first_asset_s3_object, S3Object(
        file_object=BytesIO(initial_bytes=second_asset_content),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=any_safe_filename(),
    ) as second_asset_s3_object:
        expected_hash_key = get_hash_key(dataset_id, version_id)

        metadata_stac_object = deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)
        metadata_stac_object[STAC_ASSETS_KEY] = {
            any_asset_name(): {
                LINZ_STAC_CREATED_KEY: any_past_datetime_string(),
                LINZ_STAC_UPDATED_KEY: any_past_datetime_string(),
                STAC_HREF_KEY: first_asset_s3_object.url,
                STAC_FILE_CHECKSUM_KEY: first_asset_multihash,
            },
            any_asset_name(): {
                LINZ_STAC_CREATED_KEY: any_past_datetime_string(),
                LINZ_STAC_UPDATED_KEY: any_past_datetime_string(),
                STAC_HREF_KEY: second_asset_s3_object.url,
                STAC_FILE_CHECKSUM_KEY: second_asset_multihash,
            },
        }
        with S3Object(
            file_object=json_dict_to_file_object(metadata_stac_object),
            bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
            key=any_safe_filename(),
        ) as metadata_s3_object:
            # When

            processing_assets_model = processing_assets_model_with_meta()
            expected_asset_items = [
                processing_assets_model(
                    hash_key=expected_hash_key,
                    range_key=f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}0",
                    url=first_asset_s3_object.url,
                    multihash=first_asset_multihash,
                ),
                processing_assets_model(
                    hash_key=expected_hash_key,
                    range_key=f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}1",
                    url=second_asset_s3_object.url,
                    multihash=second_asset_multihash,
                ),
            ]

            expected_metadata_items = [
                processing_assets_model(
                    hash_key=expected_hash_key,
                    range_key=f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}0",
                    url=metadata_s3_object.url,
                    exists_in_staging=True,
                ),
            ]

            assert lambda_handler(
                {
                    DATASET_ID_KEY: dataset_id,
                    VERSION_ID_KEY: version_id,
                    METADATA_URL_KEY: metadata_s3_object.url,
                    S3_ROLE_ARN_KEY: get_s3_role_arn(),
                    DATASET_PREFIX_KEY: any_dataset_prefix(),
                },
                any_lambda_context(),
            ) == {SUCCESS_KEY: True}

            # Then
            actual_asset_items = processing_assets_model.query(
                expected_hash_key,
                processing_assets_model.sk.startswith(
                    f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}"
                ),
                consistent_read=True,
            )
            for expected_item in expected_asset_items:
                with subtests.test(msg=f"Asset {expected_item.pk}"):
                    assert (
                        actual_asset_items.next().attribute_values == expected_item.attribute_values
                    )

            actual_metadata_items = processing_assets_model.query(
                expected_hash_key,
                processing_assets_model.sk.startswith(
                    f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}"
                ),
                consistent_read=True,
            )
            for expected_item in expected_metadata_items:
                with subtests.test(msg=f"Metadata {expected_item.pk}"):
                    assert (
                        actual_metadata_items.next().attribute_values
                        == expected_item.attribute_values
                    )


@mark.timeout(timedelta(minutes=20).total_seconds())
@mark.infrastructure
def should_successfully_validate_partially_uploaded_dataset(subtests: SubTests) -> None:
    # pylint: disable=too-many-locals

    key_prefix = any_safe_file_path()
    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    metadata_url_prefix = (
        f"{S3_URL_PREFIX}{Resource.STAGING_BUCKET_NAME.resource_name}/{key_prefix}"
    )
    dataset_prefix = any_dataset_prefix()
    catalog_metadata_filename = any_safe_filename()
    catalog_metadata_url = f"{metadata_url_prefix}/{catalog_metadata_filename}"
    collection_metadata_filename = any_safe_filename()
    collection_metadata_url = f"{metadata_url_prefix}/{collection_metadata_filename}"
    item_metadata_filename = any_safe_filename()
    item_metadata_url = f"{metadata_url_prefix}/{item_metadata_filename}"
    expected_hash_key = get_hash_key(dataset_id, version_id)

    with S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT),
                STAC_LINKS_KEY: [
                    {STAC_HREF_KEY: collection_metadata_url, STAC_REL_KEY: STAC_REL_CHILD},
                    {STAC_HREF_KEY: catalog_metadata_url, STAC_REL_KEY: STAC_REL_ROOT},
                    {STAC_HREF_KEY: catalog_metadata_url, STAC_REL_KEY: STAC_REL_SELF},
                ],
            }
        ),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{catalog_metadata_filename}",
    ) as catalog_metadata_file, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                STAC_LINKS_KEY: [
                    {STAC_HREF_KEY: f"./{item_metadata_filename}", STAC_REL_KEY: STAC_REL_ITEM},
                    {STAC_HREF_KEY: f"../{CATALOG_FILENAME}", STAC_REL_KEY: STAC_REL_ROOT},
                    {
                        STAC_HREF_KEY: f"./{catalog_metadata_filename}",
                        STAC_REL_KEY: STAC_REL_PARENT,
                    },
                ],
            }
        ),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=f"{dataset_prefix}/{collection_metadata_filename}",
    ), S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT),
                STAC_LINKS_KEY: [
                    {STAC_HREF_KEY: catalog_metadata_url, STAC_REL_KEY: STAC_REL_ROOT},
                    {STAC_HREF_KEY: collection_metadata_url, STAC_REL_KEY: STAC_REL_PARENT},
                    {STAC_HREF_KEY: item_metadata_url, STAC_REL_KEY: STAC_REL_SELF},
                ],
            }
        ),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{item_metadata_filename}",
    ):

        assert lambda_handler(
            {
                DATASET_ID_KEY: dataset_id,
                VERSION_ID_KEY: version_id,
                METADATA_URL_KEY: catalog_metadata_file.url,
                S3_ROLE_ARN_KEY: get_s3_role_arn(),
                DATASET_PREFIX_KEY: dataset_prefix,
            },
            any_lambda_context(),
        ) == {SUCCESS_KEY: True}

        hash_key = get_hash_key(dataset_id, version_id)
        validation_results_model = validation_results_model_with_meta()
        with subtests.test(msg="Catalog validation results"):
            assert (
                validation_results_model.get(
                    hash_key=hash_key,
                    range_key=(
                        f"{CHECK_ID_PREFIX}{Check.JSON_SCHEMA.value}"
                        f"{DB_KEY_SEPARATOR}{URL_ID_PREFIX}{catalog_metadata_url}"
                    ),
                    consistent_read=True,
                ).result
                == ValidationResult.PASSED.value
            )

            with subtests.test(msg="Collection validation results"):
                assert (
                    validation_results_model.get(
                        hash_key=hash_key,
                        range_key=(
                            f"{CHECK_ID_PREFIX}{Check.JSON_SCHEMA.value}"
                            f"{DB_KEY_SEPARATOR}{URL_ID_PREFIX}{collection_metadata_url}"
                        ),
                        consistent_read=True,
                    ).result
                    == ValidationResult.PASSED.value
                )

            with subtests.test(msg="Item validation results"):
                assert (
                    validation_results_model.get(
                        hash_key=hash_key,
                        range_key=(
                            f"{CHECK_ID_PREFIX}{Check.JSON_SCHEMA.value}"
                            f"{DB_KEY_SEPARATOR}{URL_ID_PREFIX}{item_metadata_url}"
                        ),
                        consistent_read=True,
                    ).result
                    == ValidationResult.PASSED.value
                )

            processing_assets_model = processing_assets_model_with_meta()
            expected_processing_items = [
                processing_assets_model(
                    hash_key=expected_hash_key,
                    range_key=f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}0",
                    url=catalog_metadata_url,
                    exists_in_staging=True,
                ),
                processing_assets_model(
                    hash_key=expected_hash_key,
                    range_key=f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}1",
                    url=collection_metadata_url,
                    exists_in_staging=False,
                ),
                processing_assets_model(
                    hash_key=expected_hash_key,
                    range_key=f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}2",
                    url=item_metadata_url,
                    exists_in_staging=True,
                ),
            ]
            actual_processing_items = processing_assets_model.query(
                expected_hash_key,
                processing_assets_model.sk.startswith(
                    f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}"
                ),
                consistent_read=True,
            )
            for expected_item in expected_processing_items:
                with subtests.test(msg=f"Metadata {expected_item.pk}"):
                    assert (
                        actual_processing_items.next().attribute_values
                        == expected_item.attribute_values
                    )


def should_treat_linz_example_json_files_as_valid(subtests: SubTests) -> None:
    """
    We need to make sure this repo updates the reference to the latest LINZ schema when updating the
    submodule.


    We only support fetching `s3://…` and relative URLs, so in the below we change all of them to be
    relative.
    """
    for path in glob(
        "geostore/check_stac_metadata/"
        f"{LINZ_STAC_EXTENSIONS_LOCAL_PATH}/{LINZ_SCHEMA_URL_DIRECTORY}/examples/*.json"
    ):
        with subtests.test(msg=path), open(path, encoding="utf-8") as file_handle:
            stac_object = load(file_handle)

            for asset in stac_object.get("assets", {}).values():
                asset["href"] = basename(asset["href"])

            for link in stac_object.get("links"):
                link["href"] = basename(link["href"])

            url_reader = MockJSONURLReader(
                {path: MockGeostoreS3Response(stac_object, file_in_staging=True)}
            )
            STACDatasetValidator(
                any_hash_key(), url_reader, MockValidationResultFactory()
            ).validate(path)


def should_treat_minimal_catalog_as_valid() -> None:
    STACCatalogSchemaValidator.validate(deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT))


def should_treat_minimal_collection_as_valid() -> None:
    STACCollectionSchemaValidator.validate(deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT))


def should_treat_minimal_item_as_valid() -> None:
    STACItemSchemaValidator.validate(deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT))


def should_treat_any_missing_catalog_top_level_key_as_invalid(subtests: SubTests) -> None:
    original_stac_object = MINIMAL_VALID_STAC_CATALOG_OBJECT
    for key in original_stac_object:
        with subtests.test(msg=key), raises(
            ValidationError, match=f"^'{key}' is a required property"
        ):
            stac_object = deepcopy(original_stac_object)
            stac_object.pop(key)

            STACCatalogSchemaValidator.validate(stac_object)


def should_treat_any_missing_collection_top_level_key_as_invalid(subtests: SubTests) -> None:
    original_stac_object = MINIMAL_VALID_STAC_COLLECTION_OBJECT
    for key in original_stac_object:
        stac_object = deepcopy(original_stac_object)
        stac_object.pop(key)

        with subtests.test(msg=key), raises(ValidationError):
            STACCollectionSchemaValidator.validate(stac_object)


def should_treat_any_missing_item_top_level_key_as_invalid(subtests: SubTests) -> None:
    original_stac_object = MINIMAL_VALID_STAC_ITEM_OBJECT
    for key in original_stac_object:
        with subtests.test(msg=key), raises(ValidationError):
            stac_object = deepcopy(original_stac_object)
            stac_object.pop(key)

            STACItemSchemaValidator.validate(stac_object)


def should_detect_invalid_datetime() -> None:
    stac_object = deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)
    stac_object["extent"]["temporal"]["interval"][0][0] = "not a datetime"
    with raises(ValidationError):
        STACCollectionSchemaValidator.validate(stac_object)


def should_validate_metadata_files_recursively() -> None:
    base_url = any_s3_url()
    parent_url = f"{base_url}/{any_safe_filename()}"
    child_url = f"{base_url}/{any_safe_filename()}"

    stac_object = deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)
    stac_object[STAC_LINKS_KEY].append({STAC_HREF_KEY: child_url, "rel": "child"})
    url_reader = MockJSONURLReader(
        {
            parent_url: MockGeostoreS3Response(stac_object, file_in_staging=True),
            child_url: MockGeostoreS3Response(
                deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT), file_in_staging=True
            ),
        }
    )

    with patch("geostore.check_stac_metadata.utils.processing_assets_model_with_meta"):
        STACDatasetValidator(any_hash_key(), url_reader, MockValidationResultFactory()).validate(
            parent_url
        )

    assert url_reader.mock_calls == [call(parent_url), call(child_url)]


def should_only_validate_each_file_once() -> None:
    # Given multiple references to the same URL
    # Given explicitly relative (`./foo`), implicitly relative (`foo`) and absolute URLs to the same
    # file
    base_url = any_s3_url()
    root_url = f"{base_url}/{any_safe_filename()}"
    child_filename = any_safe_filename()
    child_url = f"{base_url}/{child_filename}"
    leaf_filename = any_safe_filename()
    explicitly_relative_leaf_filename = f"./{leaf_filename}"
    leaf_url = f"{base_url}/{leaf_filename}"

    root_stac_object = deepcopy(MINIMAL_VALID_STAC_CATALOG_OBJECT)
    root_stac_object[STAC_LINKS_KEY] = [
        {STAC_HREF_KEY: child_url, "rel": "child"},
        {STAC_HREF_KEY: root_url, "rel": "root"},
        {STAC_HREF_KEY: root_url, "rel": "self"},
    ]
    child_stac_object = deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)
    child_stac_object[STAC_LINKS_KEY] = [
        {STAC_HREF_KEY: leaf_url, "rel": "child"},
        {STAC_HREF_KEY: root_url, "rel": "root"},
        {STAC_HREF_KEY: child_filename, "rel": "self"},
    ]
    leaf_stac_object = deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT)
    leaf_stac_object[STAC_LINKS_KEY] = [
        {STAC_HREF_KEY: root_url, "rel": "root"},
        {STAC_HREF_KEY: explicitly_relative_leaf_filename, "rel": "self"},
    ]
    url_reader = MockJSONURLReader(
        {
            root_url: MockGeostoreS3Response(root_stac_object, file_in_staging=True),
            child_url: MockGeostoreS3Response(child_stac_object, file_in_staging=True),
            leaf_url: MockGeostoreS3Response(leaf_stac_object, file_in_staging=True),
        },
        call_limit=3,
    )

    with patch("geostore.check_stac_metadata.utils.processing_assets_model_with_meta"):
        STACDatasetValidator(any_hash_key(), url_reader, MockValidationResultFactory()).validate(
            root_url
        )

    assert url_reader.mock_calls == [call(root_url), call(child_url), call(leaf_url)]


def should_collect_assets_from_validated_collection_metadata_files(subtests: SubTests) -> None:
    # Given one asset in another directory and one relative link
    base_url = any_s3_url()
    metadata_url = f"{base_url}/{any_safe_filename()}"
    stac_object = deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)
    first_asset_url = f"{base_url}/{any_safe_filename()}/{any_safe_filename()}"
    first_asset_multihash = any_hex_multihash()
    second_asset_filename = any_safe_filename()
    second_asset_url = f"{base_url}/{second_asset_filename}"
    second_asset_multihash = any_hex_multihash()
    stac_object[STAC_ASSETS_KEY] = {
        any_asset_name(): {
            LINZ_STAC_CREATED_KEY: any_past_datetime_string(),
            LINZ_STAC_UPDATED_KEY: any_past_datetime_string(),
            STAC_HREF_KEY: first_asset_url,
            STAC_FILE_CHECKSUM_KEY: first_asset_multihash,
        },
        any_asset_name(): {
            LINZ_STAC_CREATED_KEY: any_past_datetime_string(),
            LINZ_STAC_UPDATED_KEY: any_past_datetime_string(),
            STAC_HREF_KEY: second_asset_filename,
            STAC_FILE_CHECKSUM_KEY: second_asset_multihash,
        },
    }
    expected_assets = [
        {
            PROCESSING_ASSET_MULTIHASH_KEY: first_asset_multihash,
            PROCESSING_ASSET_URL_KEY: first_asset_url,
        },
        {
            PROCESSING_ASSET_MULTIHASH_KEY: second_asset_multihash,
            PROCESSING_ASSET_URL_KEY: second_asset_url,
        },
    ]
    file_in_staging = True
    expected_metadata = [
        {
            PROCESSING_ASSET_FILE_IN_STAGING_KEY: file_in_staging,
            PROCESSING_ASSET_URL_KEY: metadata_url,
        }
    ]
    url_reader = MockJSONURLReader(
        {metadata_url: MockGeostoreS3Response(stac_object, file_in_staging=file_in_staging)}
    )

    with patch("geostore.check_stac_metadata.utils.processing_assets_model_with_meta"):
        validator = STACDatasetValidator(any_hash_key(), url_reader, MockValidationResultFactory())

    # When
    validator.validate(metadata_url)

    # Then
    with subtests.test():
        assert _sort_assets(validator.dataset_assets) == _sort_assets(expected_assets)
    with subtests.test():
        assert validator.dataset_metadata == expected_metadata


def should_collect_assets_from_validated_item_metadata_files(subtests: SubTests) -> None:
    base_url = any_s3_url()
    metadata_url = f"{base_url}/{any_safe_filename()}"
    stac_object = deepcopy(MINIMAL_VALID_STAC_ITEM_OBJECT)
    first_asset_url = f"{base_url}/{any_safe_filename()}"
    first_asset_multihash = any_hex_multihash()
    second_asset_filename = any_safe_filename()
    second_asset_multihash = any_hex_multihash()
    stac_object[STAC_ASSETS_KEY] = {
        any_asset_name(): {
            LINZ_STAC_CREATED_KEY: any_past_datetime_string(),
            LINZ_STAC_UPDATED_KEY: any_past_datetime_string(),
            STAC_HREF_KEY: first_asset_url,
            STAC_FILE_CHECKSUM_KEY: first_asset_multihash,
        },
        any_asset_name(): {
            LINZ_STAC_CREATED_KEY: any_past_datetime_string(),
            LINZ_STAC_UPDATED_KEY: any_past_datetime_string(),
            STAC_HREF_KEY: second_asset_filename,
            STAC_FILE_CHECKSUM_KEY: second_asset_multihash,
        },
    }
    expected_assets = [
        {
            PROCESSING_ASSET_MULTIHASH_KEY: first_asset_multihash,
            PROCESSING_ASSET_URL_KEY: first_asset_url,
        },
        {
            PROCESSING_ASSET_MULTIHASH_KEY: second_asset_multihash,
            PROCESSING_ASSET_URL_KEY: f"{base_url}/{second_asset_filename}",
        },
    ]
    file_in_staging = True
    expected_metadata = [
        {
            PROCESSING_ASSET_FILE_IN_STAGING_KEY: file_in_staging,
            PROCESSING_ASSET_URL_KEY: metadata_url,
        }
    ]
    url_reader = MockJSONURLReader(
        {metadata_url: MockGeostoreS3Response(stac_object, file_in_staging=file_in_staging)}
    )

    with patch("geostore.check_stac_metadata.utils.processing_assets_model_with_meta"):
        validator = STACDatasetValidator(any_hash_key(), url_reader, MockValidationResultFactory())

    validator.validate(metadata_url)

    with subtests.test():
        assert _sort_assets(validator.dataset_assets) == _sort_assets(expected_assets)
    with subtests.test():
        assert validator.dataset_metadata == expected_metadata


def should_raise_exception_when_loading_not_unclassified_dataset(subtests: SubTests) -> None:
    metadata_url = any_s3_url()
    stac_object = deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)

    security_classification = "in-confidence"
    stac_object[LINZ_STAC_SECURITY_CLASSIFICATION_KEY] = security_classification

    url_reader = MockJSONURLReader(
        {metadata_url: MockGeostoreS3Response(stac_object, file_in_staging=True)}
    )
    mock_validation_result_factory = MockValidationResultFactory()
    validator = STACDatasetValidator(any_hash_key(), url_reader, mock_validation_result_factory)

    with subtests.test("Error raised is correct"):
        with raises(InvalidSecurityClassificationError, match=security_classification):
            validator.validate(metadata_url)

    with subtests.test("Error is part of results"):
        assert mock_validation_result_factory.mock_calls == [
            call.save(
                metadata_url,
                Check.SECURITY_CLASSIFICATION,
                ValidationResult.FAILED,
                details={
                    MESSAGE_KEY: "Expected security classification of "
                    f"'{LINZ_STAC_SECURITY_CLASSIFICATION_UNCLASSIFIED}'. "
                    f"Got '{security_classification}'."
                },
            ),
        ]


@patch("geostore.check_stac_metadata.task.ValidationResultFactory")
def should_report_invalid_json(validation_results_factory_mock: MagicMock) -> None:
    # Given
    metadata_url = any_s3_url()
    file_contents = b"{"
    url_reader = MockJSONURLReader(
        {
            metadata_url: MockGeostoreS3Response(
                StreamingBody(BytesIO(initial_bytes=file_contents), len(file_contents)),
                file_in_staging=True,
            )
        }
    )
    validator = STACDatasetValidator(any_hash_key(), url_reader, validation_results_factory_mock)

    # When
    with raises(JSONDecodeError):
        validator.validate(metadata_url)

    # Then
    assert validation_results_factory_mock.mock_calls == [
        call.save(
            metadata_url,
            Check.JSON_PARSE,
            ValidationResult.FAILED,
            details={
                MESSAGE_KEY: "Expecting property name enclosed in double quotes:"
                " line 1 column 2 (char 1)"
            },
        ),
    ]


@patch("geostore.check_stac_metadata.task.ValidationResultFactory")
def should_report_when_the_dataset_has_no_assets(
    validation_results_factory_mock: MagicMock, subtests: SubTests
) -> None:
    metadata_url = any_s3_url()
    url_reader = MockJSONURLReader(
        {
            metadata_url: MockGeostoreS3Response(
                deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT), file_in_staging=True
            )
        }
    )

    with patch("geostore.check_stac_metadata.utils.LOGGER.error") as logger_mock, subtests.test(
        msg="Logging"
    ):
        STACDatasetValidator(any_hash_key(), url_reader, validation_results_factory_mock).run(
            metadata_url
        )
        logger_mock.assert_any_call(
            LOG_MESSAGE_VALIDATION_COMPLETE,
            extra={"outcome": Outcome.FAILED, "error": NO_ASSETS_FOUND_ERROR_MESSAGE},
        )

    with subtests.test(msg="Validation results"):
        validation_results_factory_mock.save.assert_any_call(
            metadata_url,
            Check.ASSETS_IN_DATASET,
            ValidationResult.FAILED,
            details={MESSAGE_KEY: NO_ASSETS_FOUND_ERROR_MESSAGE},
        )


def _sort_assets(assets: List[Dict[str, str]]) -> List[Dict[str, str]]:
    return sorted(assets, key=lambda entry: entry[PROCESSING_ASSET_URL_KEY])
