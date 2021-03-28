import time
from copy import deepcopy
from datetime import timedelta
from hashlib import sha256
from io import BytesIO
from json import dumps

from mypy_boto3_s3 import S3Client
from mypy_boto3_s3control import S3ControlClient
from mypy_boto3_sts import STSClient
from pytest import mark
from pytest_subtests import SubTests  # type: ignore[import]
from smart_open import smart_open  # type: ignore[import]

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
    sha256_hex_digest_to_multihash,
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
    # Given two metadata files with an asset each, all within a prefix
    original_prefix = any_safe_filename()

    root_asset_name = any_asset_name()
    root_asset_filename = any_safe_filename()
    root_asset_content = any_file_contents()
    root_asset_multihash = sha256_hex_digest_to_multihash(sha256(root_asset_content).hexdigest())
    child_asset_name = any_asset_name()
    child_asset_filename = any_safe_filename()
    child_asset_content = any_file_contents()
    child_asset_multihash = sha256_hex_digest_to_multihash(sha256(child_asset_content).hexdigest())

    root_metadata_filename = any_safe_filename()
    child_metadata_filename = any_safe_filename()

    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()
    asset_id = f"DATASET#{dataset_id}#VERSION#{version_id}"

    staging_bucket_name = get_param(ParameterName.STAGING_BUCKET_NAME)
    storage_bucket_name = get_param(ParameterName.STORAGE_BUCKET_NAME)

    account = sts_client.get_caller_identity()["Account"]

    with S3Object(
        BytesIO(initial_bytes=root_asset_content),
        staging_bucket_name,
        f"{original_prefix}/{root_asset_filename}",
    ) as root_asset_s3_object, S3Object(
        BytesIO(initial_bytes=child_asset_content),
        staging_bucket_name,
        f"{original_prefix}/{child_asset_filename}",
    ) as child_asset_s3_object, S3Object(
        BytesIO(
            initial_bytes=dumps(
                {
                    **deepcopy(MINIMAL_VALID_STAC_OBJECT),
                    "assets": {
                        child_asset_name: {
                            "href": child_asset_s3_object.url,
                            "checksum:multihash": child_asset_multihash,
                        }
                    },
                }
            ).encode()
        ),
        staging_bucket_name,
        f"{original_prefix}/{child_metadata_filename}",
    ) as child_metadata_s3_object, S3Object(
        BytesIO(
            initial_bytes=dumps(
                {
                    **deepcopy(MINIMAL_VALID_STAC_OBJECT),
                    "assets": {
                        root_asset_name: {
                            "href": root_asset_s3_object.url,
                            "checksum:multihash": root_asset_multihash,
                        },
                    },
                    "links": [{"href": child_metadata_s3_object.url, "rel": "child"}],
                }
            ).encode()
        ),
        staging_bucket_name,
        f"{original_prefix}/{root_metadata_filename}",
    ) as root_metadata_s3_object, ProcessingAsset(
        asset_id=asset_id, url=root_metadata_s3_object.url
    ), ProcessingAsset(
        asset_id=asset_id, url=child_metadata_s3_object.url
    ), ProcessingAsset(
        asset_id=asset_id, url=root_asset_s3_object.url, multihash=root_asset_multihash
    ), ProcessingAsset(
        asset_id=asset_id, url=child_asset_s3_object.url, multihash=child_asset_multihash
    ):

        # When
        try:
            response = lambda_handler(
                {
                    "dataset_id": dataset_id,
                    "version_id": version_id,
                    "metadata_url": root_metadata_s3_object.url,
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
            new_prefix = f"{dataset_id}/{version_id}"

            new_root_metadata_key = f"{new_prefix}/{root_metadata_filename}"
            expected_root_metadata = dumps(
                {
                    **deepcopy(MINIMAL_VALID_STAC_OBJECT),
                    "assets": {
                        root_asset_name: {
                            "href": root_asset_s3_object.url,
                            "checksum:multihash": root_asset_multihash,
                        },
                    },
                    "links": [{"href": child_metadata_s3_object.url, "rel": "child"}],
                }
            ).encode()
            with subtests.test(msg="Root metadata content"), smart_open(
                f"s3://{storage_bucket_name}/{new_root_metadata_key}"
            ) as new_root_metadata_file:
                assert expected_root_metadata == new_root_metadata_file.read()

            with subtests.test(msg="Delete root metadata object"):
                delete_s3_key(storage_bucket_name, new_root_metadata_key, s3_client)

            new_child_metadata_key = f"{new_prefix}/{child_metadata_filename}"
            expected_child_metadata = dumps(
                {
                    **deepcopy(MINIMAL_VALID_STAC_OBJECT),
                    "assets": {
                        child_asset_name: {
                            "href": child_asset_s3_object.url,
                            "checksum:multihash": child_asset_multihash,
                        }
                    },
                }
            ).encode()
            with subtests.test(msg="Child metadata content"), smart_open(
                f"s3://{storage_bucket_name}/{new_child_metadata_key}"
            ) as new_child_metadata_file:
                assert expected_child_metadata == new_child_metadata_file.read()

            with subtests.test(msg="Delete child metadata object"):
                delete_s3_key(storage_bucket_name, new_child_metadata_key, s3_client)

            # Then the root asset file is in the root prefix
            with subtests.test(msg="Delete root asset object"):
                delete_s3_key(storage_bucket_name, f"{new_prefix}/{root_asset_filename}", s3_client)

            # Then the child asset file is in the root prefix
            with subtests.test(msg="Delete child asset object"):
                delete_s3_key(
                    storage_bucket_name, f"{new_prefix}/{child_asset_filename}", s3_client
                )

            # Cleanup
            manifest_key = s3_object_arn_to_key(
                copy_job["Job"]["Manifest"]["Location"]["ObjectArn"]
            )
            with subtests.test(msg="Delete manifest"):
                delete_s3_key(storage_bucket_name, manifest_key, s3_client)

            with subtests.test(msg="Delete job report prefix"):
                delete_s3_prefix(
                    storage_bucket_name, copy_job["Job"]["Report"]["Prefix"], s3_client
                )
