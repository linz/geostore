from logging import Logger
from os.path import basename
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import boto3
from jsonschema import ValidationError, validate
from linz_logger import get_log

from ..datasets_model import datasets_model_with_meta
from ..error_response_keys import ERROR_MESSAGE_KEY
from ..logging_keys import LOG_MESSAGE_LAMBDA_FAILURE, LOG_MESSAGE_LAMBDA_START
from ..models import DATASET_ID_PREFIX
from ..parameter_store import ParameterName, get_param
from ..resources import Resource
from ..s3 import S3_URL_PREFIX
from ..step_function_keys import (
    DATASET_ID_KEY,
    DATASET_PREFIX_KEY,
    METADATA_URL_KEY,
    NEW_VERSION_ID_KEY,
    NEW_VERSION_S3_LOCATION,
)
from ..types import JsonObject

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_sqs import SQSServiceResource
else:
    # In production we want to avoid depending on a package which has no runtime impact
    S3Client = SQSServiceResource = object  # pragma: no mutate

LOGGER: Logger = get_log()
SQS_RESOURCE: SQSServiceResource = boto3.resource("sqs")


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    """Main Lambda entry point."""
    LOGGER.debug(LOG_MESSAGE_LAMBDA_START, extra={"lambda_input": event})

    # validate input
    try:
        validate(
            event,
            {
                "type": "object",
                "properties": {
                    DATASET_ID_KEY: {"type": "string"},
                    DATASET_PREFIX_KEY: {"type": "string"},
                    NEW_VERSION_ID_KEY: {"type": "string"},
                    METADATA_URL_KEY: {"type": "string"},
                },
                "required": [
                    DATASET_ID_KEY,
                    DATASET_PREFIX_KEY,
                    METADATA_URL_KEY,
                    NEW_VERSION_ID_KEY,
                ],
            },
        )
    except ValidationError as error:
        LOGGER.warning(LOG_MESSAGE_LAMBDA_FAILURE, extra={"error": error})
        return {ERROR_MESSAGE_KEY: error.message}

    dataset_key = (
        f"{event[DATASET_PREFIX_KEY]}/{basename(urlparse(event[METADATA_URL_KEY]).path[1:])}"
    )

    # add reference to root catalog
    SQS_RESOURCE.get_queue_by_name(
        QueueName=get_param(ParameterName.UPDATE_CATALOG_MESSAGE_QUEUE_NAME)
    ).send_message(
        MessageBody=dataset_key,
    )

    # Update dataset record with the latest version
    datasets_model = datasets_model_with_meta()
    dataset = datasets_model.get(
        hash_key=f"{DATASET_ID_PREFIX}{event[DATASET_ID_KEY]}", consistent_read=True
    )
    dataset.update(actions=[datasets_model.current_dataset_version.set(event[NEW_VERSION_ID_KEY])])

    return {
        NEW_VERSION_S3_LOCATION: f"{S3_URL_PREFIX}"
        f"{Resource.STORAGE_BUCKET_NAME.resource_name}/"
        f"{dataset_key}"
    }
