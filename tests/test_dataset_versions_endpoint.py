"""
Dataset Versions endpoint Lambda function tests.
"""

from copy import deepcopy
from datetime import datetime, timezone
from http import HTTPStatus
from logging import INFO, basicConfig
from os.path import basename
from unittest.mock import MagicMock, patch

from pytest import mark
from pytest_subtests import SubTests

from geostore.api_keys import MESSAGE_KEY
from geostore.aws_keys import BODY_KEY, HTTP_METHOD_KEY, STATUS_CODE_KEY
from geostore.dataset_versions import entrypoint
from geostore.dataset_versions.create import create_dataset_version
from geostore.models import DB_KEY_SEPARATOR
from geostore.processing_assets_model import ProcessingAssetType, processing_assets_model_with_meta
from geostore.step_function import get_hash_key
from geostore.step_function_keys import (
    DATASET_ID_SHORT_KEY,
    METADATA_URL_KEY,
    NEW_VERSION_ID_KEY,
    NOW_KEY,
    S3_ROLE_ARN_KEY,
)

from .aws_utils import Dataset, ProcessingAsset, any_lambda_context, any_role_arn, any_s3_url
from .stac_generators import any_dataset_id, any_dataset_version_id

basicConfig(level=INFO)


def should_return_error_when_missing_required_property(subtests: SubTests) -> None:
    minimal_body = {
        DATASET_ID_SHORT_KEY: any_dataset_id(),
        METADATA_URL_KEY: any_s3_url(),
        S3_ROLE_ARN_KEY: any_role_arn(),
    }

    for key in minimal_body:
        with subtests.test(msg=key):
            # Given a missing property in the body
            body = deepcopy(minimal_body)
            body.pop(key)

            # When attempting to create the instance
            response = entrypoint.lambda_handler(
                {HTTP_METHOD_KEY: "POST", BODY_KEY: body}, any_lambda_context()
            )

            # Then the API should return an error message
            assert response == {
                STATUS_CODE_KEY: HTTPStatus.BAD_REQUEST,
                BODY_KEY: {MESSAGE_KEY: f"Bad Request: '{key}' is a required property"},
            }


@mark.infrastructure
def should_return_error_if_dataset_id_does_not_exist_in_db() -> None:
    body = {
        DATASET_ID_SHORT_KEY: any_dataset_id(),
        METADATA_URL_KEY: any_s3_url(),
        S3_ROLE_ARN_KEY: any_role_arn(),
    }

    response = entrypoint.lambda_handler(
        {HTTP_METHOD_KEY: "POST", BODY_KEY: body}, any_lambda_context()
    )

    assert response == {
        STATUS_CODE_KEY: HTTPStatus.NOT_FOUND,
        BODY_KEY: {
            MESSAGE_KEY: f"Not Found: dataset '{body[DATASET_ID_SHORT_KEY]}' could not be found"
        },
    }


@mark.infrastructure
def should_remove_replaced_in_new_version_field_if_exists() -> None:
    current_dataset_version = any_dataset_version_id()
    s3_url = any_s3_url()

    with Dataset(current_dataset_version=current_dataset_version) as dataset:
        current_hash_key = get_hash_key(dataset.dataset_id, current_dataset_version)
        with ProcessingAsset(
            current_hash_key,
            s3_url,
            replaced_in_new_version=True,
        ):
            processing_assets_model = processing_assets_model_with_meta()
            expected_asset_item = processing_assets_model(
                hash_key=current_hash_key,
                range_key=f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}0",
                url=s3_url,
                filename=basename(s3_url),
            )

            body = {
                DATASET_ID_SHORT_KEY: dataset.dataset_id,
                METADATA_URL_KEY: any_s3_url(),
                S3_ROLE_ARN_KEY: any_role_arn(),
            }

            # When
            create_dataset_version(body)

            # Then
            actual_asset_item = processing_assets_model.query(
                current_hash_key,
                processing_assets_model.sk.startswith(
                    f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}"
                ),
                consistent_read=True,
            ).next()
            assert actual_asset_item.attribute_values == expected_asset_item.attribute_values


@mark.infrastructure
@patch("geostore.dataset_versions.create.processing_assets_model_with_meta")
def should_do_nothing_if_replaced_in_new_version_field_is_empty(
    processing_assets_model_mock: MagicMock,
) -> None:
    current_dataset_version = any_dataset_version_id()
    s3_url = any_s3_url()

    with Dataset(current_dataset_version=current_dataset_version) as dataset:
        current_hash_key = get_hash_key(dataset.dataset_id, current_dataset_version)
        with ProcessingAsset(
            current_hash_key,
            s3_url,
        ):
            body = {
                DATASET_ID_SHORT_KEY: dataset.dataset_id,
                METADATA_URL_KEY: any_s3_url(),
                S3_ROLE_ARN_KEY: any_role_arn(),
            }

            # When requesting the dataset by ID and type
            create_dataset_version(body)

        # Then
        processing_assets_model_mock.return_value.assert_not_called()


@mark.infrastructure
def should_return_success_if_dataset_exists(subtests: SubTests) -> None:
    # Given a dataset instance
    now = datetime(2001, 2, 3, hour=4, minute=5, second=6, microsecond=789876, tzinfo=timezone.utc)

    with patch(
        "geostore.dataset_versions.create.STEP_FUNCTIONS_CLIENT.start_execution"
    ), Dataset() as dataset:
        body = {
            DATASET_ID_SHORT_KEY: dataset.dataset_id,
            METADATA_URL_KEY: any_s3_url(),
            NOW_KEY: now.isoformat(),
            S3_ROLE_ARN_KEY: any_role_arn(),
        }

        # When requesting the dataset by ID and type
        response = create_dataset_version(body)

    # Then we should get the dataset in return
    with subtests.test(msg="Status code"):
        assert response[STATUS_CODE_KEY] == HTTPStatus.CREATED

    with subtests.test(msg="ID"):
        assert response[BODY_KEY][NEW_VERSION_ID_KEY].startswith("2001-02-03T04-05-06-789Z_")
