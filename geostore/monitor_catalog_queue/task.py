from http import HTTPStatus
from json import dumps
from logging import Logger
from os import environ
from typing import TYPE_CHECKING

import boto3
from linz_logger import get_log
from slack_sdk.models.blocks import blocks
from slack_sdk.webhook.client import WebhookClient

from ..api_responses import success_response
from ..boto3_config import CONFIG
from ..logging_keys import GIT_COMMIT, LOG_MESSAGE_LAMBDA_START
from ..parameter_store import ParameterName, get_param
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

SNS_CLIENT: SNSClient = boto3.client("sns", config=CONFIG)
LOGGER: Logger = get_log()

BLOCK_MAX_CHAR_LIMIT = 3000  # https://api.slack.com/reference/block-kit/blocks#section


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    LOGGER.debug(
        LOG_MESSAGE_LAMBDA_START,
        extra={"lambda_input": event, GIT_COMMIT: get_param(ParameterName.GIT_COMMIT)},
    )

    if SLACK_URL_ENV_NAME in environ:
        post_to_slack(event)

    publish_sns_message(event)

    return success_response(HTTPStatus.OK, {})


def publish_sns_message(event: JsonObject) -> None:
    SNS_CLIENT.publish(TopicArn=get_param(ParameterName.STATUS_SNS_TOPIC_ARN), Message=dumps(event))


def post_to_slack(event: JsonObject) -> None:
    event_details = event[EVENT_DETAIL_KEY]
    slack_message_blocks = [
        blocks.HeaderBlock(text="Geostore SQS Queue > 1 within the past hour"),
        blocks.DividerBlock(),
        blocks.SectionBlock(text=f"*Status:* {event_details}"),
        blocks.DividerBlock(),
    ]

    response = WebhookClient(environ[SLACK_URL_ENV_NAME]).send(blocks=slack_message_blocks)
    assert response.status_code == HTTPStatus.OK


def format_block(title: str, body: str) -> str:
    """Perform some slack formatting and ensure blocks don't exceed character limit"""
    block_prefix = f"*{title}:* `"
    body_length = BLOCK_MAX_CHAR_LIMIT - len(block_prefix) - 1
    return f"{block_prefix}{body[:body_length]}`"
