from io import BytesIO
from json import dumps
from unittest.mock import MagicMock, patch
from urllib.parse import quote

from mypy_boto3_s3 import S3Client
from pytest import mark

from geostore.import_asset_file.task import lambda_handler
from geostore.import_dataset_file import (
    EXCEPTION_PREFIX,
    INVOCATION_ID_KEY,
    INVOCATION_SCHEMA_VERSION_KEY,
    RESULTS_KEY,
    RESULT_CODE_KEY,
    RESULT_CODE_PERMANENT_FAILURE,
    RESULT_STRING_KEY,
    S3_BUCKET_ARN_KEY,
    S3_KEY_KEY,
    TASKS_KEY,
    TASK_ID_KEY,
)
from geostore.import_dataset_keys import NEW_KEY_KEY, ORIGINAL_KEY_KEY, TARGET_BUCKET_NAME_KEY
from geostore.resources import Resource
from geostore.s3 import CHUNK_SIZE
from geostore.step_function_keys import S3_ROLE_ARN_KEY

from .aws_utils import (
    S3Object,
    any_invocation_id,
    any_invocation_schema_version,
    any_lambda_context,
    any_role_arn,
    any_s3_bucket_arn,
    any_s3_bucket_name,
    any_task_id,
    delete_s3_key,
    get_s3_role_arn,
)
from .general_generators import (
    any_error_message,
    any_file_contents,
    any_safe_file_path,
    any_safe_filename,
)


@patch("geostore.import_asset_file.task.smart_open.open")
def should_treat_unhandled_exception_as_permanent_failure(open_mock: MagicMock) -> None:
    # Given
    error_message = any_error_message()
    open_mock.side_effect = Exception(error_message)
    task_id = any_task_id()
    event = {
        TASKS_KEY: [
            {
                S3_BUCKET_ARN_KEY: any_s3_bucket_arn(),
                S3_KEY_KEY: quote(
                    dumps(
                        {
                            NEW_KEY_KEY: any_safe_file_path(),
                            ORIGINAL_KEY_KEY: any_safe_file_path(),
                            S3_ROLE_ARN_KEY: any_role_arn(),
                            TARGET_BUCKET_NAME_KEY: any_s3_bucket_name(),
                        }
                    )
                ),
                TASK_ID_KEY: task_id,
            }
        ],
        INVOCATION_ID_KEY: any_invocation_id(),
        INVOCATION_SCHEMA_VERSION_KEY: any_invocation_schema_version(),
    }

    with patch("geostore.import_dataset_file.get_s3_client_for_role"):
        response = lambda_handler(event, any_lambda_context())

    assert response[RESULTS_KEY] == [
        {
            TASK_ID_KEY: task_id,
            RESULT_CODE_KEY: RESULT_CODE_PERMANENT_FAILURE,
            RESULT_STRING_KEY: f"{EXCEPTION_PREFIX}: {error_message}",
        }
    ]


@mark.infrastructure
def should_copy_empty_file(s3_client: S3Client) -> None:
    # Given a single-chunk asset file
    target_filename = any_safe_filename()
    target_bucket = Resource.STORAGE_BUCKET_NAME.resource_name

    with S3Object(
        file_object=BytesIO(),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=any_safe_filename(),
    ) as asset_file:
        event = {
            TASKS_KEY: [
                {
                    S3_BUCKET_ARN_KEY: f"arn:aws:s3:::{asset_file.bucket_name}",
                    S3_KEY_KEY: quote(
                        dumps(
                            {
                                NEW_KEY_KEY: target_filename,
                                ORIGINAL_KEY_KEY: asset_file.key,
                                S3_ROLE_ARN_KEY: get_s3_role_arn(),
                                TARGET_BUCKET_NAME_KEY: target_bucket,
                            }
                        )
                    ),
                    TASK_ID_KEY: any_task_id(),
                }
            ],
            INVOCATION_ID_KEY: any_invocation_id(),
            INVOCATION_SCHEMA_VERSION_KEY: any_invocation_schema_version(),
        }
        try:
            # When
            lambda_handler(event, any_lambda_context())
        finally:
            # Then
            delete_s3_key(target_bucket, target_filename, s3_client)


@mark.infrastructure
def should_copy_multi_chunk_file(s3_client: S3Client) -> None:
    # Given a multi-chunk asset file
    asset_contents = any_file_contents(byte_count=CHUNK_SIZE + 1)
    target_filename = any_safe_filename()
    target_bucket = Resource.STORAGE_BUCKET_NAME.resource_name

    with S3Object(
        file_object=BytesIO(initial_bytes=asset_contents),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=any_safe_filename(),
    ) as asset_file:
        event = {
            TASKS_KEY: [
                {
                    S3_BUCKET_ARN_KEY: f"arn:aws:s3:::{asset_file.bucket_name}",
                    S3_KEY_KEY: quote(
                        dumps(
                            {
                                NEW_KEY_KEY: target_filename,
                                ORIGINAL_KEY_KEY: asset_file.key,
                                S3_ROLE_ARN_KEY: get_s3_role_arn(),
                                TARGET_BUCKET_NAME_KEY: target_bucket,
                            }
                        )
                    ),
                    TASK_ID_KEY: any_task_id(),
                }
            ],
            INVOCATION_ID_KEY: any_invocation_id(),
            INVOCATION_SCHEMA_VERSION_KEY: any_invocation_schema_version(),
        }
        try:
            # When
            lambda_handler(event, any_lambda_context())
        finally:
            # Then
            delete_s3_key(target_bucket, target_filename, s3_client)
