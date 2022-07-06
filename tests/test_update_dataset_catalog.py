from copy import deepcopy
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError
from mypy_boto3_s3 import S3Client
from pytest import mark
from pytest_subtests import SubTests

from geostore.datasets_model import datasets_model_with_meta
from geostore.error_response_keys import ERROR_MESSAGE_KEY
from geostore.models import DATASET_ID_PREFIX
from geostore.resources import Resource
from geostore.s3 import S3_URL_PREFIX
from geostore.step_function import get_hash_key
from geostore.step_function_keys import (
    CURRENT_VERSION_EMPTY_VALUE,
    CURRENT_VERSION_ID_KEY,
    DATASET_ID_KEY,
    DATASET_TITLE_KEY,
    METADATA_URL_KEY,
    NEW_VERSION_ID_KEY,
    NEW_VERSION_S3_LOCATION,
    S3_ROLE_ARN_KEY,
)
from geostore.update_root_catalog.task import lambda_handler

from .aws_utils import (
    Dataset,
    ProcessingAsset,
    S3Object,
    any_lambda_context,
    any_role_arn,
    any_s3_url,
)
from .file_utils import json_dict_to_file_object
from .general_generators import any_error_message, any_safe_filename
from .stac_generators import any_dataset_version_id
from .stac_objects import MINIMAL_VALID_STAC_COLLECTION_OBJECT


@mark.infrastructure
def should_succeed_and_trigger_sqs_catalog_update_and_save_latest_version(
    subtests: SubTests,
) -> None:
    filename = f"{any_safe_filename()}.json"

    version_id = any_dataset_version_id()

    with Dataset() as dataset, patch(
        "geostore.update_root_catalog.task.SQS_RESOURCE"
    ) as sqs_mock, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
            }
        ),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=f"{dataset.title}/{filename}",
    ) as dataset_metadata:
        response = lambda_handler(
            {
                DATASET_ID_KEY: dataset.dataset_id,
                DATASET_TITLE_KEY: dataset.title,
                CURRENT_VERSION_ID_KEY: CURRENT_VERSION_EMPTY_VALUE,
                NEW_VERSION_ID_KEY: version_id,
                METADATA_URL_KEY: f"{any_s3_url()}/{filename}",
                S3_ROLE_ARN_KEY: any_role_arn(),
            },
            any_lambda_context(),
        )

        expected_sqs_call = {"MessageBody": dataset_metadata.key}

        with subtests.test(msg="success"):
            assert response == {
                NEW_VERSION_S3_LOCATION: f"{S3_URL_PREFIX}"
                f"{Resource.STORAGE_BUCKET_NAME.resource_name}/"
                f"{dataset_metadata.key}"
            }

        with subtests.test(msg="sqs called"):
            assert sqs_mock.get_queue_by_name.return_value.send_message.called

        with subtests.test(msg="correct url passed to sqs"):
            assert (
                sqs_mock.get_queue_by_name.return_value.send_message.call_args[1]
                == expected_sqs_call
            )

            # Then
        with subtests.test(msg="Dataset updated with latest version"):
            datasets_model = datasets_model_with_meta()

            assert (
                datasets_model.get(
                    hash_key=f"{DATASET_ID_PREFIX}{dataset.dataset_id}", consistent_read=True
                ).current_dataset_version
                == version_id
            )


@mark.infrastructure
def should_delete_s3_files_that_dont_exist_in_new_version(
    s3_client: S3Client,
) -> None:

    filename = f"{any_safe_filename()}.json"
    s3_url = f"{any_s3_url()}/{filename}"

    version_id = any_dataset_version_id()
    current_version_id = any_dataset_version_id()

    with Dataset(current_dataset_version=current_version_id) as dataset, S3Object(
        file_object=json_dict_to_file_object(
            {
                **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
            }
        ),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=f"{dataset.title}/{filename}",
    ) as dataset_metadata, patch("geostore.update_root_catalog.task.SQS_RESOURCE"):

        current_hash_key = get_hash_key(dataset.dataset_id, current_version_id)

        with ProcessingAsset(
            current_hash_key,
            s3_url,
        ):
            lambda_handler(
                {
                    DATASET_ID_KEY: dataset.dataset_id,
                    DATASET_TITLE_KEY: dataset.title,
                    CURRENT_VERSION_ID_KEY: current_version_id,
                    NEW_VERSION_ID_KEY: version_id,
                    METADATA_URL_KEY: s3_url,
                    S3_ROLE_ARN_KEY: any_role_arn(),
                },
                any_lambda_context(),
            )

            # Then latest version of file is a delete marker
            assert s3_client.list_object_versions(
                Bucket=Resource.STORAGE_BUCKET_NAME.resource_name,
                Prefix=dataset_metadata.key,
            )["DeleteMarkers"][0]["IsLatest"]


@mark.infrastructure
@patch("geostore.update_root_catalog.task.validate")
def should_return_required_property_error_when_missing_mandatory_property(
    validate_url_mock: MagicMock,
) -> None:
    error_message = any_error_message()
    validate_url_mock.side_effect = ValidationError(error_message)
    with Dataset() as dataset:
        response = lambda_handler(
            {
                DATASET_ID_KEY: dataset.dataset_id,
                NEW_VERSION_ID_KEY: any_dataset_version_id(),
            },
            any_lambda_context(),
        )
        assert response == {ERROR_MESSAGE_KEY: error_message}
