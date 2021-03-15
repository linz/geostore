# FIXME: rename file to parameters.py

import json
import logging
from enum import Enum
from typing import TYPE_CHECKING

import boto3

from .environment import ENV

ssm_client = boto3.client("ssm")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# if TYPE_CHECKING:
#     # When type checking we want to use the third party package's stub
#     from mypy_boto3_ssm import SSMClient
# else:
#     # In production we want to avoid depending on a package which has no runtime impact
#     SSMClient = object


# def get_param(parameter: str, ssm_client: SSMClient, logger: logging.Logger) -> str:
def get_param(parameter: str) -> str:
    parameter_response = ssm_client.get_parameter(Name=parameter)

    try:
        return parameter_response["Parameter"]["Value"]
    except KeyError as error:
        logger.error(json.dumps({"error": error}, default=str))
        raise


class ParameterName(Enum):
    STORAGE_BUCKET_ARN = f"/{ENV}/storage-bucket-arn"
    STORAGE_BUCKET_NAME = f"/{ENV}/storage-bucket-name"
    S3_BATCH_COPY_ROLE = f"/{ENV}/s3-batch-copy-role-arn"
    PROCESSING_ASSETS_TABLE_NAME = f"/{ENV}/processing-assets-table-name"
    DATASETS_TABLE_NAME = f"/{ENV}/datasets-table-name"
    VALIDATION_RESULTS_TABLE_NAME = f"/{ENV}/validation-results-table-name"
    DATASET_STAGING_BUCKET_NAME = f"/{ENV}/dataset-staging-bucket-name"
    DATASET_VERSION_CREATION_STEP_FUNCTION_ARN = (
        f"/{ENV}/dataset-version-creation-step-function-arn"
    )
