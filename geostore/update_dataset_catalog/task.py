from json import dumps
from os.path import basename
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import boto3
from jsonschema import ValidationError, validate
from linz_logger import get_log

from ..api_keys import EVENT_KEY
from ..aws_message_attributes import (
    DATA_TYPE_STRING,
    MESSAGE_ATTRIBUTE_TYPE_DATASET,
    MESSAGE_ATTRIBUTE_TYPE_KEY,
)
from ..error_response_keys import ERROR_KEY, ERROR_MESSAGE_KEY
from ..parameter_store import ParameterName, get_param
from ..resources import Resource
from ..s3 import S3_URL_PREFIX
from ..step_function_keys import (
    DATASET_ID_KEY,
    DATASET_PREFIX_KEY,
    METADATA_URL_KEY,
    NEW_VERSION_S3_LOCATION,
    VERSION_ID_KEY,
)
from ..types import JsonObject

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_sqs import SQSServiceResource
    from mypy_boto3_sqs.type_defs import MessageAttributeValueTypeDef
else:
    # In production we want to avoid depending on a package which has no runtime impact
    S3Client = SQSServiceResource = object  # pragma: no mutate
    MessageAttributeValueTypeDef = dict  # pragma: no mutate

LOGGER = get_log()
SQS_RESOURCE: SQSServiceResource = boto3.resource("sqs")


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
                    DATASET_PREFIX_KEY: {"type": "string"},
                    VERSION_ID_KEY: {"type": "string"},
                    METADATA_URL_KEY: {"type": "string"},
                },
                "required": [DATASET_ID_KEY, DATASET_PREFIX_KEY, METADATA_URL_KEY, VERSION_ID_KEY],
            },
        )
    except ValidationError as error:
        LOGGER.warning(dumps({ERROR_KEY: error}, default=str))
        return {ERROR_MESSAGE_KEY: error.message}

    new_version_metadata_key = (
        f"{event[DATASET_PREFIX_KEY]}/{event[VERSION_ID_KEY]}/"
        f"{basename(urlparse(event[METADATA_URL_KEY]).path[1:])}"
    )

    # add reference to root catalog
    SQS_RESOURCE.get_queue_by_name(
        QueueName=get_param(ParameterName.UPDATE_CATALOG_MESSAGE_QUEUE_NAME)
    ).send_message(
        MessageBody=new_version_metadata_key,
        MessageAttributes={
            MESSAGE_ATTRIBUTE_TYPE_KEY: MessageAttributeValueTypeDef(
                DataType=DATA_TYPE_STRING, StringValue=MESSAGE_ATTRIBUTE_TYPE_DATASET
            )
        },
    )

    return {
        NEW_VERSION_S3_LOCATION: f"{S3_URL_PREFIX}"
        f"{Resource.STORAGE_BUCKET_NAME.resource_name}/"
        f"{new_version_metadata_key}"
    }
