import time
from copy import deepcopy
from datetime import timedelta
from hashlib import sha256
from io import BytesIO
from json import dumps
from urllib.parse import urlparse

import _pytest
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3control import S3ControlClient
from mypy_boto3_sts import STSClient
from pytest import mark

from backend.import_dataset.task import lambda_handler
from backend.utils import ResourceName

from .utils import (
    MINIMAL_VALID_STAC_OBJECT,
    ProcessingAsset,
    S3Object,
    any_dataset_id,
    any_dataset_version_id,
    any_file_contents,
    any_lambda_context,
    any_s3_url,
    any_safe_filename,
    any_stac_asset_name,
    any_valid_dataset_type,
)


def test_should_return_required_property_error_when_missing_metadata_url() -> None:
    # When

    response = lambda_handler(
        {"dataset_id": any_dataset_id(), "version_id": any_dataset_version_id()},
        any_lambda_context(),
    )

    assert response == {"error message": "'metadata_url' is a required property"}


def test_should_return_required_property_error_when_missing_dataset_id() -> None:
    # When
    response = lambda_handler(
        {"metadata_url": any_s3_url(), "version_id": any_dataset_version_id()}, any_lambda_context()
    )

    assert response == {"error message": "'dataset_id' is a required property"}


def test_should_return_required_property_error_when_missing_version_id() -> None:
    # When

    response = lambda_handler(
        {"dataset_id": any_dataset_id(), "metadata_url": any_s3_url()}, any_lambda_context()
    )

    assert response == {"error message": "'version_id' is a required property"}


@mark.timeout(timedelta(minutes=20).total_seconds())
@mark.infrastructure
def test_should_batch_copy_files_to_storage(
    s3_client: S3Client,
    s3_control_client: S3ControlClient,
    sts_client: STSClient,
    storage_bucket_teardown: _pytest.fixtures.FixtureDef[object],  # pylint:disable=unused-argument
) -> None:
    # pylint: disable=too-many-locals
    # Given a metadata file with an asset
    first_asset_content = any_file_contents()
    first_asset_multihash = sha256(first_asset_content).hexdigest()

    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()

    with S3Object(
        BytesIO(initial_bytes=first_asset_content),
        ResourceName.DATASET_STAGING_BUCKET_NAME.value,
        any_safe_filename(),
    ) as asset_s3_object:

        metadata_stac_object = deepcopy(MINIMAL_VALID_STAC_OBJECT)
        metadata_stac_object["assets"] = {
            any_stac_asset_name(): {
                "href": asset_s3_object.url,
                "checksum:multihash": first_asset_multihash,
            },
        }
        metadata_content = dumps(metadata_stac_object).encode()

        with S3Object(
            BytesIO(initial_bytes=metadata_content),
            ResourceName.DATASET_STAGING_BUCKET_NAME.value,
            any_safe_filename(),
        ) as metadata_s3_object:

            asset_id = f"DATASET#{dataset_id}#VERSION#{version_id}"

            with ProcessingAsset(
                asset_id=asset_id, multihash=None, url=metadata_s3_object.url
            ) as metadata_processing_asset, ProcessingAsset(
                asset_id=asset_id,
                multihash=first_asset_multihash,
                url=asset_s3_object.url,
            ) as processing_asset:

                # When

                response = lambda_handler(
                    {
                        "dataset_id": dataset_id,
                        "version_id": version_id,
                        "metadata_url": metadata_s3_object.url,
                        "type": any_valid_dataset_type(),
                    },
                    any_lambda_context(),
                )

                final_states = ["Complete", "Failed", "Cancelled"]

                # poll for S3 Batch Copy completion
                while (
                    copy_job := s3_control_client.describe_job(
                        AccountId=sts_client.get_caller_identity()["Account"],
                        JobId=response["job_id"],
                    )
                )["Job"]["Status"] not in final_states:
                    time.sleep(5)

                assert copy_job["Job"]["Status"] == "Complete", copy_job

                # Then
                for key in [metadata_processing_asset.url, processing_asset.url]:
                    s3_client.head_object(
                        Bucket=ResourceName.STORAGE_BUCKET_NAME.value,
                        Key=f"{dataset_id}/{version_id}/{urlparse(key).path[1:]}",
                    )
