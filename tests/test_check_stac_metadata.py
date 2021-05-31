import sys
from copy import deepcopy
from datetime import timedelta
from hashlib import sha256, sha512
from io import BytesIO, StringIO
from json import JSONDecodeError, dumps
from typing import Dict, List
from unittest.mock import MagicMock, call, patch

from botocore.exceptions import ClientError  # type: ignore[import]
from jsonschema import ValidationError  # type: ignore[import]
from pytest import mark, raises
from pytest_subtests import SubTests  # type: ignore[import]

from backend.api_keys import MESSAGE_KEY
from backend.check import Check
from backend.check_stac_metadata.stac_validators import STACCollectionSchemaValidator
from backend.check_stac_metadata.task import lambda_handler
from backend.check_stac_metadata.utils import (
    PROCESSING_ASSET_MULTIHASH_KEY,
    PROCESSING_ASSET_URL_KEY,
    STACDatasetValidator,
)
from backend.models import (
    CHECK_ID_PREFIX,
    DATASET_ID_PREFIX,
    DB_KEY_SEPARATOR,
    URL_ID_PREFIX,
    VERSION_ID_PREFIX,
)
from backend.parameter_store import ParameterName, get_param
from backend.processing_assets_model import ProcessingAssetType, processing_assets_model_with_meta
from backend.resources import ResourceName
from backend.s3 import S3_URL_PREFIX
from backend.stac_format import (
    STAC_ASSETS_KEY,
    STAC_DESCRIPTION_KEY,
    STAC_EXTENT_BBOX_KEY,
    STAC_EXTENT_KEY,
    STAC_EXTENT_SPATIAL_KEY,
    STAC_EXTENT_TEMPORAL_INTERVAL_KEY,
    STAC_EXTENT_TEMPORAL_KEY,
    STAC_FILE_CHECKSUM_KEY,
    STAC_HREF_KEY,
    STAC_ID_KEY,
    STAC_LICENSE_KEY,
    STAC_LINKS_KEY,
    STAC_TYPE_COLLECTION,
    STAC_TYPE_KEY,
    STAC_VERSION_KEY,
)
from backend.step_function import DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY
from backend.validation_results_model import ValidationResult, validation_results_model_with_meta

from .aws_utils import (
    MockJSONURLReader,
    MockValidationResultFactory,
    S3Object,
    any_lambda_context,
    any_s3_url,
    any_table_name,
)
from .file_utils import json_dict_to_file_object
from .general_generators import (
    any_error_message,
    any_file_contents,
    any_https_url,
    any_program_name,
    any_safe_filename,
)
from .stac_generators import (
    any_asset_name,
    any_dataset_id,
    any_dataset_version_id,
    any_hex_multihash,
)
from .stac_objects import (
    MINIMAL_VALID_STAC_CATALOG_OBJECT,
    MINIMAL_VALID_STAC_COLLECTION_OBJECT,
    MINIMAL_VALID_STAC_ITEM_OBJECT,
    STAC_VERSION,
)


@patch("backend.check_stac_metadata.task.STACDatasetValidator.validate")
def should_succeed_with_validation_failure(validate_url_mock: MagicMock) -> None:
    validate_url_mock.side_effect = ValidationError(any_error_message())

    with patch("backend.check_stac_metadata.utils.processing_assets_model_with_meta"):
        lambda_handler(
            {
                DATASET_ID_KEY: any_dataset_id(),
                VERSION_ID_KEY: any_dataset_version_id(),
                METADATA_URL_KEY: any_s3_url(),
            },
            any_lambda_context(),
        )


@patch("backend.check_stac_metadata.task.ValidationResultFactory")
@patch("backend.check_stac_metadata.task.get_param")
def should_save_non_s3_url_validation_results(
    get_param_mock: MagicMock,
    validation_results_factory_mock: MagicMock,
) -> None:

    validation_results_table_name = any_table_name()
    get_param_mock.return_value = validation_results_table_name
    non_s3_url = any_https_url()
    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    with patch("backend.check_stac_metadata.utils.processing_assets_model_with_meta"):
        lambda_handler(
            {
                DATASET_ID_KEY: dataset_id,
                VERSION_ID_KEY: version_id,
                METADATA_URL_KEY: non_s3_url,
            },
            any_lambda_context(),
        )

    hash_key = f"{DATASET_ID_PREFIX}{dataset_id}{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{version_id}"
    assert validation_results_factory_mock.mock_calls == [
        call(hash_key, validation_results_table_name),
        call().save(
            non_s3_url,
            Check.NON_S3_URL,
            ValidationResult.FAILED,
            details={MESSAGE_KEY: f"URL doesn't start with “{S3_URL_PREFIX}”: “{non_s3_url}”"},
        ),
    ]


@patch("backend.check_stac_metadata.task.ValidationResultFactory")
def should_report_duplicate_asset_names(validation_results_factory_mock: MagicMock) -> None:
    # Given
    asset_name = "name"
    metadata = (
        "{"
        f'"{STAC_ASSETS_KEY}": {{'
        f'"{asset_name}": '
        f'{{"{STAC_HREF_KEY}": "{S3_URL_PREFIX}bucket/foo", "{STAC_FILE_CHECKSUM_KEY}": ""}},'
        f'"{asset_name}": '
        f'{{"{STAC_HREF_KEY}": "{S3_URL_PREFIX}bucket/bar", "{STAC_FILE_CHECKSUM_KEY}": ""}}'
        "},"
        f'"{STAC_DESCRIPTION_KEY}": "any description",'
        f' "{STAC_EXTENT_KEY}": {{'
        f'"{STAC_EXTENT_SPATIAL_KEY}": {{"{STAC_EXTENT_BBOX_KEY}": [[-180, -90, 180, 90]]}},'
        f' "{STAC_EXTENT_TEMPORAL_KEY}":'
        f' {{"{STAC_EXTENT_TEMPORAL_INTERVAL_KEY}": [["2000-01-01T00:00:00+00:00", null]]}}'
        "},"
        f' "{STAC_ID_KEY}": "{any_dataset_id()}",'
        f' "{STAC_LICENSE_KEY}": "MIT",'
        f' "{STAC_LINKS_KEY}": [],'
        f' "{STAC_VERSION_KEY}": "{STAC_VERSION}",'
        f' "{STAC_TYPE_KEY}": "{STAC_TYPE_COLLECTION}"'
        "}"
    )
    metadata_url = any_s3_url()
    sys.argv = [
        any_program_name(),
        f"--metadata-url={metadata_url}",
        f"--dataset-id={any_dataset_id()}",
        f"--version-id={any_dataset_version_id()}",
    ]

    url_reader = MockJSONURLReader({metadata_url: StringIO(initial_value=metadata)})

    with patch("backend.check_stac_metadata.utils.processing_assets_model_with_meta"):
        # When
        STACDatasetValidator(url_reader, validation_results_factory_mock).validate(metadata_url)

    # Then
    validation_results_factory_mock.save.assert_any_call(
        metadata_url,
        Check.DUPLICATE_OBJECT_KEY,
        ValidationResult.FAILED,
        details={MESSAGE_KEY: f"Found duplicate object name “{asset_name}” in “{metadata_url}”"},
    )


@mark.infrastructure
@patch("backend.check_stac_metadata.task.S3_CLIENT.get_object")
@patch("backend.check_stac_metadata.task.ValidationResultFactory")
def should_save_staging_access_validation_results(
    validation_results_factory_mock: MagicMock,
    get_object_mock: MagicMock,
) -> None:

    validation_results_table_name = get_param(ParameterName.STORAGE_VALIDATION_RESULTS_TABLE_NAME)
    expected_error = ClientError(
        {"Error": {"Code": "TEST", "Message": "TEST"}}, operation_name="get_object"
    )
    get_object_mock.side_effect = expected_error

    s3_url = any_s3_url()
    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    lambda_handler(
        {
            DATASET_ID_KEY: dataset_id,
            VERSION_ID_KEY: version_id,
            METADATA_URL_KEY: s3_url,
        },
        any_lambda_context(),
    )

    hash_key = f"{DATASET_ID_PREFIX}{dataset_id}{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{version_id}"
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
def should_save_json_schema_validation_results_per_file(subtests: SubTests) -> None:
    base_url = f"{S3_URL_PREFIX}{ResourceName.STAGING_BUCKET_NAME.value}/"
    valid_child_key = any_safe_filename()
    invalid_child_key = any_safe_filename()
    invalid_stac_object = deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)
    invalid_stac_object.pop("id")

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
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=any_safe_filename(),
    ) as root_s3_object, S3Object(
        file_object=json_dict_to_file_object(deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)),
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=valid_child_key,
    ) as valid_child_s3_object, S3Object(
        file_object=json_dict_to_file_object(invalid_stac_object),
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=invalid_child_key,
    ) as invalid_child_s3_object:

        # When
        lambda_handler(
            {
                DATASET_ID_KEY: dataset_id,
                VERSION_ID_KEY: version_id,
                METADATA_URL_KEY: root_s3_object.url,
            },
            any_lambda_context(),
        )

    hash_key = f"{DATASET_ID_PREFIX}{dataset_id}{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{version_id}"
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
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=any_safe_filename(),
    ) as first_asset_s3_object, S3Object(
        file_object=BytesIO(initial_bytes=second_asset_content),
        bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
        key=any_safe_filename(),
    ) as second_asset_s3_object:
        expected_hash_key = (
            f"{DATASET_ID_PREFIX}{dataset_id}{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{version_id}"
        )

        metadata_stac_object = deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)
        metadata_stac_object[STAC_ASSETS_KEY] = {
            any_asset_name(): {
                STAC_HREF_KEY: first_asset_s3_object.url,
                STAC_FILE_CHECKSUM_KEY: first_asset_multihash,
            },
            any_asset_name(): {
                STAC_HREF_KEY: second_asset_s3_object.url,
                STAC_FILE_CHECKSUM_KEY: second_asset_multihash,
            },
        }
        metadata_content = dumps(metadata_stac_object).encode()
        with S3Object(
            file_object=BytesIO(initial_bytes=metadata_content),
            bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
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
                ),
            ]

            lambda_handler(
                {
                    DATASET_ID_KEY: dataset_id,
                    VERSION_ID_KEY: version_id,
                    METADATA_URL_KEY: metadata_s3_object.url,
                },
                any_lambda_context(),
            )

            # Then
            actual_items = processing_assets_model.query(
                expected_hash_key,
                processing_assets_model.sk.startswith(
                    f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}"
                ),
            )
            for actual_item, expected_item in zip(actual_items, expected_asset_items):
                with subtests.test():
                    assert actual_item.attribute_values == expected_item.attribute_values

            actual_items = processing_assets_model.query(
                expected_hash_key,
                processing_assets_model.sk.startswith(
                    f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}"
                ),
            )
            for actual_item, expected_item in zip(actual_items, expected_metadata_items):
                with subtests.test():
                    assert actual_item.attribute_values == expected_item.attribute_values


@patch("backend.check_stac_metadata.task.STACDatasetValidator.validate")
def should_validate_given_url(validate_url_mock: MagicMock) -> None:
    url = any_s3_url()

    with patch("backend.check_stac_metadata.utils.processing_assets_model_with_meta"):
        lambda_handler(
            {
                DATASET_ID_KEY: any_dataset_id(),
                VERSION_ID_KEY: any_dataset_version_id(),
                METADATA_URL_KEY: url,
            },
            any_lambda_context(),
        )

    validate_url_mock.assert_called_once_with(url)


def should_treat_minimal_stac_object_as_valid() -> None:
    STACCollectionSchemaValidator().validate(deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT))


def should_treat_any_missing_top_level_key_as_invalid(subtests: SubTests) -> None:
    for stac_object in [
        MINIMAL_VALID_STAC_COLLECTION_OBJECT,
        MINIMAL_VALID_STAC_ITEM_OBJECT,
        MINIMAL_VALID_STAC_CATALOG_OBJECT,
    ]:
        for key in stac_object:
            with subtests.test(msg=f"{stac_object[STAC_TYPE_KEY]} {key}"):
                stac_object = deepcopy(stac_object)
                stac_object.pop(key)

                with raises(ValidationError):
                    STACCollectionSchemaValidator().validate(stac_object)


def should_detect_invalid_datetime() -> None:
    stac_object = deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)
    stac_object["extent"]["temporal"]["interval"][0][0] = "not a datetime"
    with raises(ValidationError):
        STACCollectionSchemaValidator().validate(stac_object)


def should_validate_metadata_files_recursively() -> None:
    base_url = any_s3_url()
    parent_url = f"{base_url}/{any_safe_filename()}"
    child_url = f"{base_url}/{any_safe_filename()}"

    stac_object = deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)
    stac_object[STAC_LINKS_KEY].append({STAC_HREF_KEY: child_url, "rel": "child"})
    url_reader = MockJSONURLReader(
        {parent_url: stac_object, child_url: deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)}
    )

    with patch("backend.check_stac_metadata.utils.processing_assets_model_with_meta"):
        STACDatasetValidator(url_reader, MockValidationResultFactory()).validate(parent_url)

    assert url_reader.mock_calls == [call(parent_url), call(child_url)]


def should_only_validate_each_file_once() -> None:
    # Given multiple references to the same URL
    # Given relative and absolute URLs to the same file
    base_url = any_s3_url()
    root_url = f"{base_url}/{any_safe_filename()}"
    child_filename = any_safe_filename()
    child_url = f"{base_url}/{child_filename}"
    leaf_url = f"{base_url}/{any_safe_filename()}"

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
        {STAC_HREF_KEY: leaf_url, "rel": "self"},
    ]
    url_reader = MockJSONURLReader(
        {root_url: root_stac_object, child_url: child_stac_object, leaf_url: leaf_stac_object},
        call_limit=3,
    )

    with patch("backend.check_stac_metadata.utils.processing_assets_model_with_meta"):
        STACDatasetValidator(url_reader, MockValidationResultFactory()).validate(root_url)

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
            STAC_HREF_KEY: first_asset_url,
            STAC_FILE_CHECKSUM_KEY: first_asset_multihash,
        },
        any_asset_name(): {
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
    expected_metadata = [{PROCESSING_ASSET_URL_KEY: metadata_url}]
    url_reader = MockJSONURLReader({metadata_url: stac_object})

    with patch("backend.check_stac_metadata.utils.processing_assets_model_with_meta"):
        validator = STACDatasetValidator(url_reader, MockValidationResultFactory())

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
            STAC_HREF_KEY: first_asset_url,
            STAC_FILE_CHECKSUM_KEY: first_asset_multihash,
        },
        any_asset_name(): {
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
    expected_metadata = [{PROCESSING_ASSET_URL_KEY: metadata_url}]
    url_reader = MockJSONURLReader({metadata_url: stac_object})

    with patch("backend.check_stac_metadata.utils.processing_assets_model_with_meta"):
        validator = STACDatasetValidator(url_reader, MockValidationResultFactory())

    validator.validate(metadata_url)

    with subtests.test():
        assert _sort_assets(validator.dataset_assets) == _sort_assets(expected_assets)
    with subtests.test():
        assert validator.dataset_metadata == expected_metadata


@patch("backend.check_stac_metadata.task.ValidationResultFactory")
def should_report_invalid_json(validation_results_factory_mock: MagicMock) -> None:
    # Given
    metadata_url = any_s3_url()
    url_reader = MockJSONURLReader({metadata_url: StringIO(initial_value="{")})
    validator = STACDatasetValidator(url_reader, validation_results_factory_mock)

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


def _sort_assets(assets: List[Dict[str, str]]) -> List[Dict[str, str]]:
    return sorted(assets, key=lambda entry: entry[PROCESSING_ASSET_URL_KEY])
