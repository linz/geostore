import time
from copy import deepcopy
from datetime import timedelta
from hashlib import sha256
from io import BytesIO
from json import dumps
from urllib.parse import urlparse

from mypy_boto3_s3 import S3Client
from mypy_boto3_s3control import S3ControlClient
from mypy_boto3_sts import STSClient
from pytest import mark
from pytest_subtests import SubTests  # type: ignore[import]

from backend.import_dataset.task import lambda_handler
from backend.parameter_store import ParameterName, get_param

from .aws_utils import (
    MINIMAL_VALID_STAC_OBJECT,
    S3_BATCH_JOB_COMPLETED_STATE,
    S3_BATCH_JOB_FINAL_STATES,
    ProcessingAsset,
    S3Object,
    any_lambda_context,
    any_s3_url,
    delete_s3_key,
    delete_s3_prefix,
    s3_object_arn_to_key,
)
from .general_generators import any_file_contents, any_safe_filename
from .stac_generators import (
    any_asset_name,
    any_dataset_id,
    any_dataset_version_id,
    any_valid_dataset_type,
)


def should_return_required_property_error_when_missing_metadata_url() -> None:
    # When

    response = lambda_handler(
        {"dataset_id": any_dataset_id(), "version_id": any_dataset_version_id()},
        any_lambda_context(),
    )

    assert response == {"error message": "'metadata_url' is a required property"}


def should_return_required_property_error_when_missing_dataset_id() -> None:
    # When
    response = lambda_handler(
        {"metadata_url": any_s3_url(), "version_id": any_dataset_version_id()}, any_lambda_context()
    )

    assert response == {"error message": "'dataset_id' is a required property"}


def should_return_required_property_error_when_missing_version_id() -> None:
    # When

    response = lambda_handler(
        {"dataset_id": any_dataset_id(), "metadata_url": any_s3_url()}, any_lambda_context()
    )

    assert response == {"error message": "'version_id' is a required property"}


@mark.timeout(timedelta(minutes=20).total_seconds())
@mark.infrastructure
def should_batch_copy_files_to_storage(
    s3_client: S3Client,
    s3_control_client: S3ControlClient,
    sts_client: STSClient,
    subtests: SubTests,
) -> None:
    # pylint: disable=too-many-locals
    # Given a metadata file with an asset
    asset_content = any_file_contents()
    asset_multihash = sha256(asset_content).hexdigest()

    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()
    asset_id = f"DATASET#{dataset_id}#VERSION#{version_id}"

    staging_bucket_name = get_param(ParameterName.STAGING_BUCKET_NAME)
    storage_bucket_name = get_param(ParameterName.STORAGE_BUCKET_NAME)

    account = sts_client.get_caller_identity()["Account"]

    with S3Object(
        BytesIO(initial_bytes=asset_content), staging_bucket_name, any_safe_filename()
    ) as asset_s3_object, S3Object(
        BytesIO(
            initial_bytes=dumps(
                {
                    **deepcopy(MINIMAL_VALID_STAC_OBJECT),
                    "assets": {
                        any_asset_name(): {
                            "href": asset_s3_object.url,
                            "checksum:multihash": asset_multihash,
                        },
                    },
                }
            ).encode()
        ),
        staging_bucket_name,
        any_safe_filename(),
    ) as metadata_s3_object, ProcessingAsset(
        asset_id=asset_id, multihash=None, url=metadata_s3_object.url
    ) as metadata_processing_asset, ProcessingAsset(
        asset_id=asset_id, multihash=asset_multihash, url=asset_s3_object.url
    ) as processing_asset:

        # When
        try:
            response = lambda_handler(
                {
                    "dataset_id": dataset_id,
                    "version_id": version_id,
                    "metadata_url": metadata_s3_object.url,
                    "type": any_valid_dataset_type(),
                },
                any_lambda_context(),
            )

            # poll for S3 Batch Copy completion
            while (
                copy_job := s3_control_client.describe_job(
                    AccountId=account, JobId=response["job_id"]
                )
            )["Job"]["Status"] not in S3_BATCH_JOB_FINAL_STATES:
                time.sleep(5)

            assert copy_job["Job"]["Status"] == S3_BATCH_JOB_COMPLETED_STATE, copy_job
        finally:
            # Then
            for original_url in [metadata_processing_asset.url, processing_asset.url]:
                new_key = f"{dataset_id}/{version_id}/{urlparse(original_url).path[1:]}"
                with subtests.test(msg=f"Delete {new_key}"):
                    delete_s3_key(storage_bucket_name, new_key, s3_client)

            manifest_key = s3_object_arn_to_key(
                copy_job["Job"]["Manifest"]["Location"]["ObjectArn"]
            )
            with subtests.test(msg=f"Delete {manifest_key}"):
                delete_s3_key(storage_bucket_name, manifest_key, s3_client)

            copy_job_report_prefix = copy_job["Job"]["Report"]["Prefix"]
            with subtests.test(msg=f"Delete {copy_job_report_prefix}"):
                delete_s3_prefix(storage_bucket_name, copy_job_report_prefix, s3_client)
