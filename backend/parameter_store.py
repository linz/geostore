import json
import logging
from enum import Enum
from typing import TYPE_CHECKING

from .environment import ENV

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_ssm import SSMClient
else:
    # In production we want to avoid depending on a package which has no runtime impact
    SSMClient = object


class ParameterName(Enum):
    DATASET_VERSION_CREATION_STEP_FUNCTION = f"/{ENV}/step-func-statemachine-arn"
    S3_BATCH_COPY_ROLE_PARAMETER_NAME = f"/{ENV}/s3-batch-copy-role-arn"
    STORAGE_BUCKET_PARAMETER_NAME = f"/{ENV}/storage-bucket-arn"


def get_param(parameter: ParameterName, ssm_client: SSMClient, logger: logging.Logger) -> str:
    parameter_response = ssm_client.get_parameter(Name=parameter.value)

    try:
        return parameter_response["Parameter"]["Value"]
    except KeyError as error:
        logger.error(json.dumps({"error": error}, default=str))
        raise
