from datetime import datetime
from http import HTTPStatus
from json import dumps, loads
from os import environ
from typing import TYPE_CHECKING

import boto3
from linz_logger import get_log
from slack_sdk.models.blocks import blocks
from slack_sdk.webhook.client import WebhookClient

from ..api_keys import EVENT_KEY
from ..api_responses import success_response
from ..aws_message_attributes import DATA_TYPE_STRING
from ..boto3_config import CONFIG
from ..parameter_store import ParameterName, get_param
from ..step_function import get_import_status_given_arn
from ..step_function_keys import (
    ASSET_UPLOAD_KEY,
    DATASET_PREFIX_KEY,
    INPUT_KEY,
    JOB_STATUS_SUCCEEDED,
    METADATA_UPLOAD_KEY,
    NEW_VERSION_S3_LOCATION,
    OUTPUT_KEY,
    STATUS_KEY,
    UPDATE_DATASET_KEY,
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
    SNSClient = object  # pragma: no mutate
    MessageAttributeValueTypeDef = dict  # pragma: no mutate


SLACK_URL_ENV_NAME = "GEOSTORE_SLACK_NOTIFY_URL"

EVENT_DETAIL_KEY = "detail"

MESSAGE_ATTRIBUTE_DATASET_KEY = "dataset_id"
MESSAGE_ATTRIBUTE_STATUS_KEY = "status"

STEP_FUNCTION_ARN_KEY = "executionArn"
STEP_FUNCTION_STARTDATE_KEY = "startDate"
STEP_FUNCTION_STOPDATE_KEY = "stopDate"

WEBHOOK_MESSAGE_BLOCKS_KEY = "blocks"
WEBHOOK_MESSAGE_CHANNEL_KEY = "channel"

SNS_CLIENT: SNSClient = boto3.client("sns", config=CONFIG)
LOGGER = get_log()

BLOCK_MAX_CHAR_LIMIT = 3000  # https://api.slack.com/reference/block-kit/blocks#section


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    LOGGER.debug(dumps({EVENT_KEY: event}))

    if (SLACK_URL_ENV_NAME in environ) and (event[EVENT_DETAIL_KEY][STEP_FUNCTION_STOPDATE_KEY]):
        post_to_slack(event)

    publish_sns_message(event)

    return success_response(HTTPStatus.OK, {})


def publish_sns_message(event: JsonObject) -> None:
    dataset_id = loads(event[EVENT_DETAIL_KEY][INPUT_KEY])[DATASET_PREFIX_KEY]
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
    step_function_input = loads(event_details[INPUT_KEY])
    validation_details = get_import_status_given_arn(event_details[STEP_FUNCTION_ARN_KEY])

    running_time = str(
        datetime.fromtimestamp(event_details[STEP_FUNCTION_STOPDATE_KEY] / 1000)
        - datetime.fromtimestamp(event_details[STEP_FUNCTION_STARTDATE_KEY] / 1000)
    ).split(".", maxsplit=1)[0]

    slack_message_blocks = [
        blocks.HeaderBlock(text="Geostore Dataset Version Import"),
        blocks.DividerBlock(),
        blocks.SectionBlock(text=f"*Status:* {event_details[STATUS_KEY]}"),
        blocks.DividerBlock(),
        blocks.SectionBlock(text=f"*Dataset ID:* `{step_function_input[DATASET_PREFIX_KEY]}`"),
        blocks.SectionBlock(text=f"*Dataset Version ID:* `{step_function_input[VERSION_ID_KEY]}`"),
        blocks.SectionBlock(text=f"*Execution ARN:* `{event_details[STEP_FUNCTION_ARN_KEY]}`"),
        blocks.DividerBlock(),
        blocks.SectionBlock(text=f"*Running Time:* {running_time}"),
        blocks.DividerBlock(),
        blocks.SectionBlock(
            text=format_block("Validation", dumps(validation_details[VALIDATION_KEY]))
        ),
        blocks.SectionBlock(
            text=format_block("Asset Upload", dumps(validation_details[ASSET_UPLOAD_KEY]))
        ),
        blocks.SectionBlock(
            text=format_block("Metadata Upload", dumps(validation_details[METADATA_UPLOAD_KEY]))
        ),
    ]

    if event_details[STATUS_KEY] == JOB_STATUS_SUCCEEDED:
        step_function_output = loads(event_details[OUTPUT_KEY])
        s3_location = step_function_output[UPDATE_DATASET_KEY][NEW_VERSION_S3_LOCATION]
        slack_message_blocks.append(blocks.DividerBlock())
        slack_message_blocks.append(blocks.SectionBlock(text=f"*S3 Location:* `{s3_location}`"))

    response = WebhookClient(environ[SLACK_URL_ENV_NAME]).send(blocks=slack_message_blocks)
    assert response.status_code == HTTPStatus.OK


def format_block(title: str, body: str) -> str:
    """Perform some slack formatting and ensure blocks don't exceed character limit"""
    block_prefix = f"*{title}:* `"
    body_length = BLOCK_MAX_CHAR_LIMIT - len(block_prefix) - 1
    return f"{block_prefix}{body[:body_length]}`"
