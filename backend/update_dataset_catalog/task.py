from json import dumps
from os.path import basename

import boto3
from jsonschema import ValidationError, validate  # type: ignore[import]
from pynamodb.exceptions import DoesNotExist

from backend.datasets_model import datasets_model_with_meta
from backend.error_response_keys import ERROR_KEY, ERROR_MESSAGE_KEY
from backend.import_dataset.task import EVENT_KEY
from backend.log import set_up_logging
from backend.models import DATASET_ID_PREFIX
from backend.parameter_store import ParameterName, get_param
from backend.s3 import s3_url_to_key
from backend.sqs_message_attributes import (
    MESSAGE_ATTRIBUTE_TYPE_DATASET,
    MESSAGE_ATTRIBUTE_TYPE_KEY,
)
from backend.step_function import DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY
from backend.types import JsonObject

LOGGER = set_up_logging(__name__)
S3_CLIENT = boto3.client("s3")
SQS_RESOURCE = boto3.resource("sqs")


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    """Main Lambda entry point."""
    LOGGER.debug(dumps({EVENT_KEY: event}))

    # validate input
    try:
        validate(
            event,
            {
                "type": "object",
                "properties": {
                    DATASET_ID_KEY: {"type": "string"},
                    VERSION_ID_KEY: {"type": "string"},
                    METADATA_URL_KEY: {"type": "string"},
                },
                "required": [DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY],
            },
        )
    except ValidationError as error:
        LOGGER.warning(dumps({ERROR_KEY: error}, default=str))
        return {ERROR_MESSAGE_KEY: error.message}

    datasets_model_class = datasets_model_with_meta()

    # get dataset
    try:
        dataset = datasets_model_class.get(
            hash_key=f"{DATASET_ID_PREFIX}{event[DATASET_ID_KEY]}", consistent_read=True
        )
    except DoesNotExist:
        return {ERROR_MESSAGE_KEY: f"dataset '{event[DATASET_ID_KEY]}' could not be found"}

    new_version_metadata_key = (
        f"{dataset.dataset_prefix}{event[VERSION_ID_KEY]}"
        f"{basename(s3_url_to_key(event[METADATA_URL_KEY]))}"
    )

    # add reference to root catalog
    SQS_RESOURCE.get_queue_by_name(
        QueueName=get_param(ParameterName.UPDATE_CATALOG_MESSAGE_QUEUE_NAME)
    ).send_message(
        MessageBody=new_version_metadata_key,
        MessageAttributes={
            MESSAGE_ATTRIBUTE_TYPE_KEY: {
                "StringValue": MESSAGE_ATTRIBUTE_TYPE_DATASET,
                "DataType": "String",
            }
        },
    )

    return {}
