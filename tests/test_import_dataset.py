import time
from copy import deepcopy
from datetime import timedelta
from hashlib import sha256
from io import BytesIO
from json import dumps
from typing import Any, Dict
from urllib.parse import urlparse

from mypy_boto3_s3 import S3Client
from mypy_boto3_s3control import S3ControlClient
from mypy_boto3_sts import STSClient
from pytest import mark

from ..endpoints.utils import ResourceName
from ..processing.import_dataset.task import lambda_handler
from .utils import (
    ProcessingAsset,
    S3Object,
    any_dataset_description,
    any_dataset_id,
    any_dataset_version_id,
    any_file_contents,
    any_lambda_context,
    any_past_datetime_string,
    any_safe_filename,
    any_stac_asset_name,
    any_valid_dataset_type,
)

STAC_VERSION = "1.0.0-beta.2"

MINIMAL_VALID_STAC_OBJECT: Dict[str, Any] = {
    "stac_version": STAC_VERSION,
    "id": any_dataset_id(),
    "description": any_dataset_description(),
    "links": [],
    "license": "MIT",
    "extent": {
        "spatial": {"bbox": [[-180, -90, 180, 90]]},
        "temporal": {"interval": [[any_past_datetime_string(), None]]},
    },
}


@mark.timeout(timedelta(minutes=20).total_seconds())
@mark.infrastructure
def test_should_batch_copy_files_to_storage(
    s3_client: S3Client,
    s3_control_client: S3ControlClient,
    sts_client: STSClient,
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
                asset_id=asset_id, multihash=None, item_index="0", url=metadata_s3_object.url
            ) as metadata_processing_asset, ProcessingAsset(
                asset_id=asset_id,
                multihash=first_asset_multihash,
                item_index="1",
                url=asset_s3_object.url,
            ) as processing_asset:

                # When
                body = {}
                body["dataset_id"] = dataset_id
                body["version_id"] = version_id
                body["metadata_url"] = metadata_s3_object.url
                body["type"] = any_valid_dataset_type()

                response = lambda_handler(
                    {"httpMethod": "POST", "body": body}, any_lambda_context()
                )

                final_states = ["Complete", "Failed", "Cancelled"]
                # poll for S3 Batch Copy completion
                while (
                    copy_job := s3_control_client.describe_job(
                        AccountId=sts_client.get_caller_identity()["Account"],
                        JobId=response["body"]["JobId"],
                    )
                )["Job"]["Status"] not in final_states:
                    time.sleep(5)

                assert copy_job["Job"]["Status"] == "Complete", copy_job

                # Then
                for key in [metadata_processing_asset.url, processing_asset.url]:
                    s3_client.head_object(
                        Bucket=ResourceName.STORAGE_BUCKET_NAME.value, Key=urlparse(key).path[1:]
                    )
