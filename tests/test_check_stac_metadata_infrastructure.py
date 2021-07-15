from copy import deepcopy
from datetime import timedelta
from hashlib import sha256, sha512
from io import BytesIO
from json import dumps
from os import environ
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, call, patch

from botocore.exceptions import ClientError
from pytest import mark
from pytest_subtests import SubTests

from backend.aws_keys import AWS_DEFAULT_REGION_KEY

from .aws_profile_utils import any_region_name

if TYPE_CHECKING:
    from botocore.exceptions import (  # pylint:disable=no-name-in-module,ungrouped-imports
        ClientErrorResponseError,
        ClientErrorResponseTypeDef,
    )
else:
    ClientErrorResponseError = ClientErrorResponseTypeDef = dict

with patch.dict(
    environ, {AWS_DEFAULT_REGION_KEY: environ.get(AWS_DEFAULT_REGION_KEY, any_region_name())}
):
    from backend.api_keys import MESSAGE_KEY
    from backend.check import Check
    from backend.check_stac_metadata.task import lambda_handler
    from backend.models import (
        CHECK_ID_PREFIX,
        DATASET_ID_PREFIX,
        DB_KEY_SEPARATOR,
        URL_ID_PREFIX,
        VERSION_ID_PREFIX,
    )
    from backend.parameter_store import ParameterName, get_param
    from backend.processing_assets_model import (
        ProcessingAssetType,
        processing_assets_model_with_meta,
    )
    from backend.resources import ResourceName
    from backend.s3 import S3_URL_PREFIX
    from backend.stac_format import (
        STAC_ASSETS_KEY,
        STAC_FILE_CHECKSUM_KEY,
        STAC_HREF_KEY,
        STAC_ID_KEY,
        STAC_LINKS_KEY,
    )
    from backend.step_function_keys import (
        DATASET_ID_KEY,
        METADATA_URL_KEY,
        S3_ROLE_ARN_KEY,
        VERSION_ID_KEY,
    )
    from backend.validation_results_model import (
        ValidationResult,
        validation_results_model_with_meta,
    )

    from .aws_utils import S3Object, any_lambda_context, any_role_arn, any_s3_url, get_s3_role_arn
    from .file_utils import json_dict_to_file_object
    from .general_generators import any_file_contents, any_safe_filename
    from .stac_generators import any_asset_name, any_dataset_id, any_dataset_version_id
    from .stac_objects import MINIMAL_VALID_STAC_COLLECTION_OBJECT


@mark.infrastructure
@patch("backend.check_stac_metadata.task.get_s3_client_for_role")
@patch("backend.check_stac_metadata.task.ValidationResultFactory")
def should_save_staging_access_validation_results(
    validation_results_factory_mock: MagicMock,
    get_s3_client_for_role_mock: MagicMock,
) -> None:
    validation_results_table_name = get_param(ParameterName.STORAGE_VALIDATION_RESULTS_TABLE_NAME)
    expected_error = ClientError(
        ClientErrorResponseTypeDef(Error=ClientErrorResponseError(Code="TEST", Message="TEST")),
        operation_name="get_object",
    )
    get_s3_client_for_role_mock.return_value.get_object.side_effect = expected_error

    s3_url = any_s3_url()
    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    lambda_handler(
        {
            DATASET_ID_KEY: dataset_id,
            VERSION_ID_KEY: version_id,
            METADATA_URL_KEY: s3_url,
            S3_ROLE_ARN_KEY: any_role_arn(),
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
        assert (
            lambda_handler(
                {
                    DATASET_ID_KEY: dataset_id,
                    VERSION_ID_KEY: version_id,
                    METADATA_URL_KEY: root_s3_object.url,
                    S3_ROLE_ARN_KEY: get_s3_role_arn(),
                },
                any_lambda_context(),
            )
            == {}
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
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
        key=any_safe_filename(),
    ) as first_asset_s3_object, S3Object(
        file_object=BytesIO(initial_bytes=second_asset_content),
        bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
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
            bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
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

            assert (
                lambda_handler(
                    {
                        DATASET_ID_KEY: dataset_id,
                        VERSION_ID_KEY: version_id,
                        METADATA_URL_KEY: metadata_s3_object.url,
                        S3_ROLE_ARN_KEY: get_s3_role_arn(),
                    },
                    any_lambda_context(),
                )
                == {}
            )

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
