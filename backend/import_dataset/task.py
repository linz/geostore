import json
from urllib.parse import urlparse
from uuid import uuid4

import boto3
from jsonschema import ValidationError, validate  # type: ignore[import]
from smart_open import open as smart_open  # type: ignore[import]

from ..log import set_up_logging
from ..parameter_store import ParameterName, get_param
from ..processing_assets_model import ProcessingAssetsModel
from ..types import JsonObject

STS_CLIENT = boto3.client("sts")
S3_CLIENT = boto3.client("s3")
S3CONTROL_CLIENT = boto3.client("s3control")
SSM_CLIENT = boto3.client("ssm")


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    """Main Lambda entry point."""
    # pylint: disable=too-many-locals

    logger = set_up_logging(__name__)
    logger.debug(json.dumps({"event": event}))

    # validate input
    try:
        validate(
            event,
            {
                "type": "object",
                "properties": {
                    "dataset_id": {"type": "string"},
                    "version_id": {"type": "string"},
                    "metadata_url": {"type": "string"},
                },
                "required": ["dataset_id", "metadata_url", "version_id"],
            },
        )
    except ValidationError as error:
        logger.warning(json.dumps({"error": error}, default=str))
        return {"error message": error.message}

    dataset_id = event["dataset_id"]
    dataset_version_id = event["version_id"]
    metadata_url = event["metadata_url"]

    storage_bucket_arn = get_param(ParameterName.STORAGE_BUCKET_PARAMETER_NAME, SSM_CLIENT, logger)
    storage_bucket_name = storage_bucket_arn.rsplit(":", maxsplit=1)[-1]

    staging_bucket_name = urlparse(metadata_url).netloc
    manifest_key = f"manifests/{dataset_version_id}.csv"

    with smart_open(f"s3://{storage_bucket_name}/{manifest_key}", "w") as s3_manifest:
        for item in ProcessingAssetsModel.query(
            f"DATASET#{dataset_id}#VERSION#{dataset_version_id}"
        ):
            logger.debug(json.dumps({"Adding file to manifest": item.url}))
            key = urlparse(item.url).path[1:]
            s3_manifest.write(f"{staging_bucket_name},{key}\n")

    caller_identity = STS_CLIENT.get_caller_identity()
    assert "Account" in caller_identity, caller_identity
    account_number = caller_identity["Account"]

    manifest_s3_object = S3_CLIENT.head_object(Bucket=storage_bucket_name, Key=manifest_key)
    assert "ETag" in manifest_s3_object, manifest_s3_object
    manifest_s3_etag = manifest_s3_object["ETag"]

    s3_batch_copy_role_arn = get_param(
        ParameterName.S3_BATCH_COPY_ROLE_PARAMETER_NAME, SSM_CLIENT, logger
    )

    # trigger s3 batch copy operation
    response = S3CONTROL_CLIENT.create_job(
        AccountId=account_number,
        ConfirmationRequired=False,
        Operation={
            "S3PutObjectCopy": {
                "TargetResource": storage_bucket_arn,
                "TargetKeyPrefix": f"{dataset_id}/{dataset_version_id}",
            }
        },
        Manifest={
            "Spec": {
                "Format": "S3BatchOperations_CSV_20180820",
                "Fields": ["Bucket", "Key"],
            },
            "Location": {
                "ObjectArn": f"{storage_bucket_arn}/{manifest_key}",
                "ETag": manifest_s3_etag,
            },
        },
        Report={
            "Enabled": True,
            "Bucket": storage_bucket_arn,
            "Format": "Report_CSV_20180820",
            "Prefix": f"reports/{dataset_version_id}",
            "ReportScope": "AllTasks",
        },
        Priority=1,
        RoleArn=s3_batch_copy_role_arn,
        ClientRequestToken=uuid4().hex,
    )
    logger.debug(json.dumps({"s3 batch response": response}, default=str))

    return {"job_id": response["JobId"]}
