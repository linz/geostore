from json import dumps
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError
from pytest import mark
from pytest_subtests import SubTests

from geostore.api_keys import EVENT_KEY
from geostore.error_response_keys import ERROR_KEY
from geostore.import_dataset.task import lambda_handler
from geostore.models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR, VERSION_ID_PREFIX
from geostore.step_function_keys import (
    DATASET_ID_KEY,
    DATASET_PREFIX_KEY,
    METADATA_URL_KEY,
    S3_BATCH_RESPONSE_KEY,
    S3_ROLE_ARN_KEY,
    VERSION_ID_KEY,
)

from .aws_utils import Dataset, ProcessingAsset, any_lambda_context, any_role_arn, any_s3_url
from .general_generators import any_error_message, any_etag
from .stac_generators import any_dataset_version_id, any_hex_multihash


@patch("geostore.import_dataset.task.S3_CLIENT.head_object")
@mark.infrastructure
def should_log_payload(head_object_mock: MagicMock) -> None:
    # Given
    head_object_mock.return_value = {"ETag": any_etag()}

    with patch(
        "geostore.import_dataset.task.S3CONTROL_CLIENT.create_job"
    ), Dataset() as dataset, patch(
        "geostore.import_dataset.task.LOGGER.debug"
    ) as logger_mock, patch(
        "geostore.import_dataset.task.validate"
    ), patch(
        "geostore.import_dataset.task.smart_open.open"
    ):
        event = {
            DATASET_ID_KEY: dataset.dataset_id,
            DATASET_PREFIX_KEY: dataset.dataset_prefix,
            METADATA_URL_KEY: any_s3_url(),
            S3_ROLE_ARN_KEY: any_role_arn(),
            VERSION_ID_KEY: any_dataset_version_id(),
        }
        expected_payload_log = dumps({EVENT_KEY: event})

        # When
        lambda_handler(event, any_lambda_context())

        # Then
        logger_mock.assert_any_call(expected_payload_log)


@patch("geostore.import_dataset.task.validate")
def should_log_schema_validation_warning(validate_schema_mock: MagicMock) -> None:
    # Given

    error_message = any_error_message()
    validate_schema_mock.side_effect = ValidationError(error_message)
    expected_log = dumps({ERROR_KEY: error_message})

    with patch("geostore.import_dataset.task.LOGGER.warning") as logger_mock:
        # When
        lambda_handler({}, any_lambda_context())

        # Then
        logger_mock.assert_any_call(expected_log)


@patch("geostore.import_dataset.task.S3_CLIENT.head_object")
@mark.infrastructure
def should_log_assets_added_to_manifest(
    head_object_mock: MagicMock,
    subtests: SubTests,
) -> None:
    # Given
    with Dataset() as dataset:
        version_id = any_dataset_version_id()
        asset_id = (
            f"{DATASET_ID_PREFIX}{dataset.dataset_id}"
            f"{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{version_id}"
        )
        head_object_mock.return_value = {"ETag": any_etag()}

        with ProcessingAsset(
            asset_id=asset_id, multihash=None, url=any_s3_url()
        ) as metadata_processing_asset, ProcessingAsset(
            asset_id=asset_id,
            multihash=any_hex_multihash(),
            url=any_s3_url(),
        ) as processing_asset, patch(
            "geostore.import_dataset.task.LOGGER.debug"
        ) as logger_mock, patch(
            "geostore.import_dataset.task.smart_open.open"
        ), patch(
            "geostore.import_dataset.task.S3CONTROL_CLIENT.create_job"
        ):

            expected_asset_log = dumps({"Adding file to manifest": processing_asset.url})
            expected_metadata_log = dumps(
                {"Adding file to manifest": metadata_processing_asset.url}
            )

            # When
            lambda_handler(
                {
                    DATASET_ID_KEY: dataset.dataset_id,
                    DATASET_PREFIX_KEY: dataset.dataset_prefix,
                    METADATA_URL_KEY: any_s3_url(),
                    S3_ROLE_ARN_KEY: any_role_arn(),
                    VERSION_ID_KEY: version_id,
                },
                any_lambda_context(),
            )

            # Then
            with subtests.test():
                logger_mock.assert_any_call(expected_asset_log)
            with subtests.test():
                logger_mock.assert_any_call(expected_metadata_log)


@patch("geostore.import_dataset.task.S3CONTROL_CLIENT.create_job")
@patch("geostore.import_dataset.task.S3_CLIENT.head_object")
@mark.infrastructure
def should_log_s3_batch_response(head_object_mock: MagicMock, create_job_mock: MagicMock) -> None:
    # Given

    create_job_mock.return_value = response = {"JobId": "Some Response"}
    expected_response_log = dumps({S3_BATCH_RESPONSE_KEY: response})
    head_object_mock.return_value = {"ETag": any_etag()}

    with Dataset() as dataset, patch(
        "geostore.import_dataset.task.LOGGER.debug"
    ) as logger_mock, patch("geostore.import_dataset.task.smart_open.open"):

        # When
        lambda_handler(
            {
                DATASET_ID_KEY: dataset.dataset_id,
                DATASET_PREFIX_KEY: dataset.dataset_prefix,
                METADATA_URL_KEY: any_s3_url(),
                S3_ROLE_ARN_KEY: any_role_arn(),
                VERSION_ID_KEY: any_dataset_version_id(),
            },
            any_lambda_context(),
        )

        # Then
        logger_mock.assert_any_call(expected_response_log)
