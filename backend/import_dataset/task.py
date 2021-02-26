import json
import logging
from datetime import datetime
from http.client import responses as http_responses
from os import environ
from typing import Any, List, MutableMapping, Union
from urllib.parse import urlparse
from uuid import uuid4

import boto3
from jsonschema import ValidationError, validate  # type: ignore[import]
from smart_open import open as smart_open  # type: ignore[import]

from ..model import ProcessingAssetsModel

JsonList = List[Any]
JsonObject = MutableMapping[str, Any]

ssm_client = boto3.client("ssm")
sts_client = boto3.client("sts")
s3_client = boto3.client("s3")
s3control_client = boto3.client("s3control")

ENV = environ.get("DEPLOY_ENV", "test")
STORAGE_BUCKET_PARAMETER = f"/{ENV}/storage-bucket-arn"
S3_BATCH_COPY_ROLE_PARAMETER = f"/{ENV}/s3-batch-copy-role-arn"


BODY_SCHEMA = {
    "type": "object",
    "properties": {
        "dataset_id": {"type": "string"},
        "version_id": {"type": "string"},
        "metadata_url": {"type": "string"},
    },
    "required": ["dataset_id", "metadata_url", "version_id"],
}


def lambda_handler(payload: JsonObject, _context: bytes) -> JsonObject:
    """Main Lambda entry point."""

    logger = set_up_logging()
    logger.debug(json.dumps({"payload": payload}))

    # validate input
    try:
        validate(payload, BODY_SCHEMA)
    except ValidationError as error:
        return error_response(400, error.message, logger)

    dataset_id = payload["dataset_id"]
    dataset_version_id = payload["version_id"]
    metadata_url = payload["metadata_url"]

    storage_bucket_arn = get_param(STORAGE_BUCKET_PARAMETER)
    storage_bucket_name = storage_bucket_arn.rsplit(":", maxsplit=1)[-1]

    staging_bucket_name = urlparse(metadata_url).netloc
    manifest_key = f"manifests/{dataset_version_id}.csv"

    with smart_open(f"s3://{storage_bucket_name}/{manifest_key}", "w") as s3_manifest:
        for file in ProcessingAssetsModel.query(
            f"DATASET#{dataset_id}#VERSION#{dataset_version_id}"
        ):
            logger.debug(json.dumps({"Adding file to manifest": file.url}, default=str))
            key = urlparse(file.url).path[1:]
            s3_manifest.write(f"{staging_bucket_name},{key}\n")

    # trigger s3 batch copy operation
    response = s3control_client.create_job(
        AccountId=sts_client.get_caller_identity()["Account"],
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
                "Fields": [
                    "Bucket",
                    "Key",
                ],
            },
            "Location": {
                "ObjectArn": f"{storage_bucket_arn}/{manifest_key}",
                "ETag": s3_client.head_object(Bucket=storage_bucket_name, Key=manifest_key)["ETag"],
            },
        },
        Report={
            "Enabled": True,
            "Bucket": storage_bucket_arn,
            "Format": "Report_CSV_20180820",
            "Prefix": f"reports/{datetime.now()}-{dataset_version_id}",
            "ReportScope": "AllTasks",
        },
        Priority=1,
        RoleArn=get_param(S3_BATCH_COPY_ROLE_PARAMETER),
        ClientRequestToken=uuid4().hex,
    )
    logger.debug(json.dumps({"s3 batch response": response}, default=str))

    return success_response(200, {"job_id": response["JobId"]})


def set_up_logging() -> logging.Logger:
    logger = logging.getLogger(__name__)

    log_handler = logging.StreamHandler()
    log_level = environ.get("LOGLEVEL", logging.NOTSET)

    logger.addHandler(log_handler)
    logger.setLevel(log_level)

    return logger


def error_response(code: int, message: str, logger: logging.Logger) -> JsonObject:
    """Return error response content as string."""
    logger.warning(json.dumps({"error": message}, default=str))
    return {"statusCode": code, "body": {"message": f"{http_responses[code]}: {message}"}}


def success_response(code: int, body: Union[JsonList, JsonObject]) -> JsonObject:
    """Return success response content as string."""

    return {"statusCode": code, "body": body}


def get_param(parameter: str) -> str:
    parameter_response = ssm_client.get_parameter(Name=parameter)

    try:
        parameter = parameter_response["Parameter"]["Value"]
    except KeyError:
        print(parameter_response)
        raise

    return parameter
