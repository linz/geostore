from datetime import datetime
from http import HTTPStatus
from json import dumps, loads
from os import environ
from os.path import basename
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import boto3
from slack_sdk.models.blocks import blocks
from slack_sdk.webhook import WebhookClient

from ..api_keys import EVENT_KEY
from ..aws_message_attributes import DATA_TYPE_STRING
from ..log import set_up_logging
from ..parameter_store import ParameterName, get_param
from ..resources import ResourceName
from ..s3 import S3_URL_PREFIX
from ..step_function_keys import (
    ASSET_UPLOAD_KEY,
    DATASET_PREFIX_KEY,
    METADATA_UPLOAD_KEY,
    METADATA_URL_KEY,
    STATUS_KEY,
    VALIDATION_KEY,
    VERSION_ID_KEY,
)
from ..types import JsonObject

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_sns import SNSClient
    from mypy_boto3_sns.type_defs import MessageAttributeValueTypeDef
else:
    # In production we want to avoid depending on a package which has no runtime impact
    SNSClient = object
    MessageAttributeValueTypeDef = dict


SLACK_URL_ENV_NAME = "GEOSTORE_SLACK_NOTIFY_URL"

EVENT_DETAIL_KEY = "detail"

MESSAGE_ATTRIBUTE_DATASET_KEY = "dataset_id"
MESSAGE_ATTRIBUTE_STATUS_KEY = "status"

STEP_FUNCTION_ARN_KEY = "executionArn"
STEP_FUNCTION_INPUT_KEY = "input"
STEP_FUNCTION_OUTPUT_KEY = "output"
STEP_FUNCTION_STARTDATE_KEY = "startDate"
STEP_FUNCTION_UPLOAD_STATUS_KEY = "upload_status"
STEP_FUNCTION_UPDATE_DATASET_KEY = "update_dataset_catalog"
STEP_FUNCTION_STOPDATE_KEY = "stopDate"

WEBHOOK_MESSAGE_BLOCKS_KEY = "blocks"
WEBHOOK_MESSAGE_CHANNEL_KEY = "channel"

SNS_CLIENT: SNSClient = boto3.client("sns")
LOGGER = set_up_logging(__name__)


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    LOGGER.debug(dumps({EVENT_KEY: event}))

    if (SLACK_URL_ENV_NAME in environ) and (event[EVENT_DETAIL_KEY][STEP_FUNCTION_STOPDATE_KEY]):
        post_to_slack(event)

    publish_sns_message(event)

    return {}


def publish_sns_message(event: JsonObject) -> None:
    dataset_id = loads(event[EVENT_DETAIL_KEY][STEP_FUNCTION_INPUT_KEY])[DATASET_PREFIX_KEY]
    SNS_CLIENT.publish(
        TopicArn=get_param(ParameterName.STATUS_SNS_TOPIC_ARN),
        Message=dumps(event),
        MessageAttributes={
            MESSAGE_ATTRIBUTE_DATASET_KEY: MessageAttributeValueTypeDef(
                DataType=DATA_TYPE_STRING, StringValue=dataset_id
            ),
            MESSAGE_ATTRIBUTE_STATUS_KEY: MessageAttributeValueTypeDef(
                DataType=DATA_TYPE_STRING, StringValue=event[EVENT_DETAIL_KEY][STATUS_KEY]
            ),
        },
    )


def post_to_slack(event: JsonObject) -> None:
    event_details = event[EVENT_DETAIL_KEY]
    step_function_input = loads(event_details[STEP_FUNCTION_INPUT_KEY])
    step_function_output = loads(event_details[STEP_FUNCTION_OUTPUT_KEY])
    validation_details = step_function_output[STEP_FUNCTION_UPLOAD_STATUS_KEY]
    running_time = str(
        datetime.fromtimestamp(event_details[STEP_FUNCTION_STOPDATE_KEY] / 1000.0)
        - datetime.fromtimestamp(event_details[STEP_FUNCTION_STARTDATE_KEY] / 1000.0)
    )

    new_version_metadata_key = (
        f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}/"
        f"{step_function_input[DATASET_PREFIX_KEY]}/{step_function_input[VERSION_ID_KEY]}/"
        f"{basename(urlparse(step_function_input[METADATA_URL_KEY]).path[1:])}"
    )

    slack_message_blocks = [
        blocks.HeaderBlock(text="Geostore Dataset Version Import"),
        blocks.DividerBlock(),
        blocks.SectionBlock(text=f"*Status:* {event_details[STATUS_KEY]}"),
        blocks.DividerBlock(),
        blocks.SectionBlock(
            text=f"*Dataset ID:* `{step_function_input[DATASET_PREFIX_KEY]}`\n"
            f"*Dataset Version ID:* `{step_function_input[VERSION_ID_KEY]}`\n"
            f"*Execution ARN:* `{event_details[STEP_FUNCTION_ARN_KEY]}`"
        ),
        blocks.DividerBlock(),
        blocks.SectionBlock(text=f"*Running Time:* {running_time}"),
        blocks.DividerBlock(),
        blocks.SectionBlock(
            text=f"*Validation:* `{dumps(validation_details[VALIDATION_KEY])}`\n"
            f"*Asset Upload:* `{dumps(validation_details[ASSET_UPLOAD_KEY])}`\n"
            f"*Metadata Upload:* `{dumps(validation_details[METADATA_UPLOAD_KEY])}`"
        ),
        blocks.DividerBlock(),
        blocks.SectionBlock(text=f"*S3 Location:* `{new_version_metadata_key}`"),
    ]

    response = WebhookClient(environ[SLACK_URL_ENV_NAME]).send(blocks=slack_message_blocks)
    assert response.status_code == HTTPStatus.OK
