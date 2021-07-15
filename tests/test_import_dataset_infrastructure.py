from copy import deepcopy
from datetime import timedelta
from hashlib import sha256
from io import BytesIO
from json import dumps
from os import environ
from unittest.mock import patch

from mypy_boto3_s3 import S3Client
from mypy_boto3_s3control import S3ControlClient
from pytest import mark
from pytest_subtests import SubTests
from smart_open import open as smart_open

from backend.aws_keys import AWS_DEFAULT_REGION_KEY
from backend.datasets_model import DATASET_KEY_SEPARATOR
from backend.models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR, VERSION_ID_PREFIX
from backend.resources import ResourceName
from backend.s3 import S3_URL_PREFIX
from backend.stac_format import (
    STAC_ASSETS_KEY,
    STAC_FILE_CHECKSUM_KEY,
    STAC_HREF_KEY,
    STAC_LINKS_KEY,
)
from backend.step_function_keys import (
    DATASET_ID_KEY,
    DATASET_PREFIX_KEY,
    METADATA_URL_KEY,
    S3_ROLE_ARN_KEY,
    VERSION_ID_KEY,
)
from backend.sts import get_account_number

from .aws_profile_utils import any_region_name
from .aws_utils import (
    Dataset,
    ProcessingAsset,
    S3Object,
    any_lambda_context,
    delete_copy_job_files,
    delete_s3_key,
    get_s3_role_arn,
    wait_for_copy_jobs,
)
from .general_generators import any_file_contents, any_safe_filename
from .stac_generators import any_asset_name, any_dataset_version_id, sha256_hex_digest_to_multihash
from .stac_objects import MINIMAL_VALID_STAC_COLLECTION_OBJECT

with patch.dict(
    environ, {AWS_DEFAULT_REGION_KEY: environ.get(AWS_DEFAULT_REGION_KEY, any_region_name())}
):
    from backend.import_dataset.task import lambda_handler


@mark.timeout(timedelta(minutes=20).total_seconds())
@mark.infrastructure
def should_batch_copy_files_to_storage(
    s3_client: S3Client,
    s3_control_client: S3ControlClient,
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

    with S3Object(
        BytesIO(initial_bytes=root_asset_content),
        ResourceName.STAGING_BUCKET_NAME.value,
        f"{original_prefix}/{root_asset_filename}",
    ) as root_asset_s3_object, S3Object(
        BytesIO(initial_bytes=child_asset_content),
        ResourceName.STAGING_BUCKET_NAME.value,
        f"{original_prefix}/{child_asset_filename}",
    ) as child_asset_s3_object, S3Object(
        BytesIO(
            initial_bytes=dumps(
                {
                    **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                    STAC_ASSETS_KEY: {
                        child_asset_name: {
                            STAC_HREF_KEY: f"./{child_asset_filename}",
                            STAC_FILE_CHECKSUM_KEY: child_asset_multihash,
                        }
                    },
                }
            ).encode()
        ),
        ResourceName.STAGING_BUCKET_NAME.value,
        f"{original_prefix}/{child_metadata_filename}",
    ) as child_metadata_s3_object, S3Object(
        BytesIO(
            initial_bytes=dumps(
                {
                    **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                    STAC_ASSETS_KEY: {
                        root_asset_name: {
                            STAC_HREF_KEY: root_asset_s3_object.url,
                            STAC_FILE_CHECKSUM_KEY: root_asset_multihash,
                        },
                    },
                    STAC_LINKS_KEY: [{STAC_HREF_KEY: child_metadata_s3_object.url, "rel": "child"}],
                }
            ).encode()
        ),
        ResourceName.STAGING_BUCKET_NAME.value,
        f"{original_prefix}/{root_metadata_filename}",
    ) as root_metadata_s3_object, Dataset() as dataset:
        version_id = any_dataset_version_id()
        asset_id = (
            f"{DATASET_ID_PREFIX}{dataset.dataset_id}"
            f"{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{version_id}"
        )

        with ProcessingAsset(asset_id=asset_id, url=root_metadata_s3_object.url), ProcessingAsset(
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
                        DATASET_ID_KEY: dataset.dataset_id,
                        DATASET_PREFIX_KEY: dataset.dataset_prefix,
                        VERSION_ID_KEY: version_id,
                        METADATA_URL_KEY: root_metadata_s3_object.url,
                        S3_ROLE_ARN_KEY: get_s3_role_arn(),
                    },
                    any_lambda_context(),
                )

                account_id = get_account_number()

                metadata_copy_job_result, asset_copy_job_result = wait_for_copy_jobs(
                    response,
                    account_id,
                    s3_control_client,
                    subtests,
                )
            finally:
                # Then
                new_prefix = (
                    f"{dataset.title}{DATASET_KEY_SEPARATOR}{dataset.dataset_id}/{version_id}"
                )
                storage_bucket_prefix = f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}/"

                new_root_metadata_key = f"{new_prefix}/{root_metadata_filename}"
                expected_root_metadata = dumps(
                    {
                        **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                        STAC_ASSETS_KEY: {
                            root_asset_name: {
                                STAC_HREF_KEY: root_asset_filename,
                                STAC_FILE_CHECKSUM_KEY: root_asset_multihash,
                            },
                        },
                        STAC_LINKS_KEY: [{STAC_HREF_KEY: child_metadata_filename, "rel": "child"}],
                    }
                ).encode()
                with subtests.test(msg="Root metadata content"), smart_open(
                    f"{storage_bucket_prefix}{new_root_metadata_key}", mode="rb"
                ) as new_root_metadata_file:
                    assert expected_root_metadata == new_root_metadata_file.read()

                with subtests.test(msg="Delete root metadata object"):
                    delete_s3_key(
                        ResourceName.STORAGE_BUCKET_NAME.value, new_root_metadata_key, s3_client
                    )

                new_child_metadata_key = f"{new_prefix}/{child_metadata_filename}"
                expected_child_metadata = dumps(
                    {
                        **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
                        STAC_ASSETS_KEY: {
                            child_asset_name: {
                                STAC_HREF_KEY: child_asset_filename,
                                STAC_FILE_CHECKSUM_KEY: child_asset_multihash,
                            }
                        },
                    }
                ).encode()
                with subtests.test(msg="Child metadata content"), smart_open(
                    f"{storage_bucket_prefix}{new_child_metadata_key}", mode="rb"
                ) as new_child_metadata_file:
                    assert expected_child_metadata == new_child_metadata_file.read()

                with subtests.test(msg="Delete child metadata object"):
                    delete_s3_key(
                        ResourceName.STORAGE_BUCKET_NAME.value, new_child_metadata_key, s3_client
                    )

                # Then the root asset file is in the root prefix
                new_root_asset_key = f"{new_prefix}/{root_asset_filename}"

                with subtests.test(msg="Verify root asset contents"), smart_open(
                    f"{storage_bucket_prefix}{new_root_asset_key}", mode="rb"
                ) as new_root_asset_file:
                    assert root_asset_content == new_root_asset_file.read()

                with subtests.test(msg="Delete root asset object"):
                    delete_s3_key(
                        ResourceName.STORAGE_BUCKET_NAME.value,
                        new_root_asset_key,
                        s3_client,
                    )

                # Then the child asset file is in the root prefix
                new_child_asset_key = f"{new_prefix}/{child_asset_filename}"

                with subtests.test(msg="Verify child asset contents"), smart_open(
                    f"{storage_bucket_prefix}{new_child_asset_key}", mode="rb"
                ) as new_child_asset_file:
                    assert child_asset_content == new_child_asset_file.read()

                with subtests.test(msg="Delete child asset object"):
                    delete_s3_key(
                        ResourceName.STORAGE_BUCKET_NAME.value,
                        new_child_asset_key,
                        s3_client,
                    )

                # Cleanup
                delete_copy_job_files(
                    metadata_copy_job_result,
                    asset_copy_job_result,
                    ResourceName.STORAGE_BUCKET_NAME.value,
                    s3_client,
                    subtests,
                )
