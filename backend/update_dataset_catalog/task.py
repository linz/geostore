from json import dumps
from os.path import basename
from urllib.parse import urlparse

import boto3
from jsonschema import ValidationError, validate  # type: ignore[import]
from pynamodb.exceptions import DoesNotExist

from ..api_keys import EVENT_KEY
from ..datasets_model import datasets_model_with_meta
from ..error_response_keys import ERROR_KEY, ERROR_MESSAGE_KEY
from ..log import set_up_logging
from ..models import DATASET_ID_PREFIX
from ..parameter_store import ParameterName, get_param
from ..sqs_message_attributes import (
    DATA_TYPE_KEY,
    DATA_TYPE_STRING,
    MESSAGE_ATTRIBUTE_TYPE_DATASET,
    MESSAGE_ATTRIBUTE_TYPE_KEY,
    STRING_VALUE_KEY,
)
from ..step_function import DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY
from ..types import JsonObject

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
        f"{dataset.dataset_prefix}/{event[VERSION_ID_KEY]}/"
        f"{basename(urlparse(event[METADATA_URL_KEY]).path[1:])}"
    )

    # add reference to root catalog
    SQS_RESOURCE.get_queue_by_name(
        QueueName=get_param(ParameterName.UPDATE_CATALOG_MESSAGE_QUEUE_NAME)
    ).send_message(
        MessageBody=new_version_metadata_key,
        MessageAttributes={
            MESSAGE_ATTRIBUTE_TYPE_KEY: {
                STRING_VALUE_KEY: MESSAGE_ATTRIBUTE_TYPE_DATASET,
                DATA_TYPE_KEY: DATA_TYPE_STRING,
            }
        },
    )

    return {}
