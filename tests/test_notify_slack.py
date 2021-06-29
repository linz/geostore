from datetime import datetime, timedelta
from http import HTTPStatus
from json import dumps
from logging import getLogger
from os import environ
from unittest.mock import MagicMock, patch

from mypy_boto3_sns.type_defs import MessageAttributeValueTypeDef
from pytest_subtests import SubTests

from backend.api_keys import EVENT_KEY
from backend.aws_message_attributes import DATA_TYPE_STRING
from backend.notify_slack.task import (
    EVENT_DETAIL_KEY,
    MESSAGE_ATTRIBUTE_DATASET_KEY,
    MESSAGE_ATTRIBUTE_STATUS_KEY,
    SLACK_URL_ENV_NAME,
    STEP_FUNCTION_ARN_KEY,
    STEP_FUNCTION_INPUT_KEY,
    STEP_FUNCTION_OUTPUT_KEY,
    STEP_FUNCTION_STARTDATE_KEY,
    STEP_FUNCTION_STOPDATE_KEY,
    STEP_FUNCTION_UPLOAD_STATUS_KEY,
    WEBHOOK_MESSAGE_BLOCKS_KEY,
    lambda_handler,
    post_to_slack,
    publish_sns_message,
)
from backend.step_function_keys import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_KEY,
    JOB_STATUS_SUCCEEDED,
    METADATA_UPLOAD_KEY,
    STATUS_KEY,
    VALIDATION_KEY,
    VERSION_ID_KEY,
)

from .aws_utils import any_arn_formatted_string, any_lambda_context
from .general_generators import any_https_url
from .stac_generators import any_dataset_id, any_dataset_version_id


@patch("backend.notify_slack.task.WebhookClient.send")
def should_notify_slack_with_finished_details_when_url_set(webhook_client_mock: MagicMock) -> None:
    # Given

    webhook_client_mock.return_value.status_code = HTTPStatus.OK
    mock_slack_url = any_https_url()

    with patch.dict(environ, {SLACK_URL_ENV_NAME: mock_slack_url}):
        now_ts = datetime.now()
        # When
        notify_slack_input = {
            EVENT_DETAIL_KEY: {
                STATUS_KEY: JOB_STATUS_SUCCEEDED,
                STEP_FUNCTION_ARN_KEY: any_arn_formatted_string(),
                STEP_FUNCTION_INPUT_KEY: dumps(
                    {
                        DATASET_ID_KEY: any_dataset_id(),
                        VERSION_ID_KEY: any_dataset_version_id(),
                    }
                ),
                STEP_FUNCTION_OUTPUT_KEY: dumps(
                    {
                        STEP_FUNCTION_UPLOAD_STATUS_KEY: {
                            VALIDATION_KEY: "",
                            ASSET_UPLOAD_KEY: "",
                            METADATA_UPLOAD_KEY: "",
                        }
                    }
                ),
                STEP_FUNCTION_STARTDATE_KEY: (now_ts - timedelta(seconds=10)).timestamp(),
                STEP_FUNCTION_STOPDATE_KEY: now_ts.timestamp(),
            }
        }

        post_to_slack(notify_slack_input)

        # Then
        webhook_client_mock.assert_called_once()
        assert len(webhook_client_mock.call_args[1][WEBHOOK_MESSAGE_BLOCKS_KEY]) == 9


@patch("backend.notify_slack.task.WebhookClient.send")
def should_notify_slack_when_url_set(webhook_client_mock: MagicMock) -> None:
    # Given

    webhook_client_mock.return_value.status_code = HTTPStatus.OK
    mock_slack_url = any_https_url()

    with patch.dict(environ, {SLACK_URL_ENV_NAME: mock_slack_url}):
        now_ts = datetime.now()
        # When
        notify_slack_input = {
            EVENT_DETAIL_KEY: {
                STATUS_KEY: JOB_STATUS_SUCCEEDED,
                STEP_FUNCTION_ARN_KEY: any_arn_formatted_string(),
                STEP_FUNCTION_INPUT_KEY: dumps(
                    {
                        DATASET_ID_KEY: any_dataset_id(),
                        VERSION_ID_KEY: any_dataset_version_id(),
                    }
                ),
                STEP_FUNCTION_OUTPUT_KEY: None,
                STEP_FUNCTION_STARTDATE_KEY: (now_ts - timedelta(seconds=10)).timestamp(),
                STEP_FUNCTION_STOPDATE_KEY: None,
            }
        }

        post_to_slack(notify_slack_input)

        # Then
        webhook_client_mock.assert_called_once()
        assert len(webhook_client_mock.call_args[1][WEBHOOK_MESSAGE_BLOCKS_KEY]) == 6


@patch("backend.notify_slack.task.WebhookClient.send")
def should_log_and_not_post_to_slack_when_url_not_set(
    webhook_client_mock: MagicMock, subtests: SubTests
) -> None:
    # Given
    logger = getLogger("backend.notify_slack.task")

    with patch("backend.notify_slack.task.publish_sns_message"), patch.object(
        logger, "debug"
    ) as logger_mock:
        # When
        lambda_handler({}, any_lambda_context())

    # Then
    with subtests.test("no slack message"):
        assert not webhook_client_mock.called

    with subtests.test("log created"):
        expected_log = dumps({EVENT_KEY: {}})
        logger_mock.assert_any_call(expected_log)


@patch("backend.notify_slack.task.get_param")
def should_publish_sns_message(get_param_mock: MagicMock) -> None:
    # Given
    get_param_mock.return_value = topic_arn = any_arn_formatted_string()
    dataset_id = any_dataset_id()
    publish_sns_message_input = {
        EVENT_DETAIL_KEY: {
            STATUS_KEY: JOB_STATUS_SUCCEEDED,
            STEP_FUNCTION_INPUT_KEY: dumps(
                {
                    DATASET_ID_KEY: dataset_id,
                }
            ),
        }
    }

    expected_sns_call = {
        "TopicArn": topic_arn,
        "Message": dumps(publish_sns_message_input),
        "MessageAttributes": {
            MESSAGE_ATTRIBUTE_DATASET_KEY: MessageAttributeValueTypeDef(
                DataType=DATA_TYPE_STRING, StringValue=dataset_id
            ),
            MESSAGE_ATTRIBUTE_STATUS_KEY: MessageAttributeValueTypeDef(
                DataType=DATA_TYPE_STRING, StringValue=JOB_STATUS_SUCCEEDED
            ),
        },
    }

    # When
    with patch("backend.notify_slack.task.SNS_CLIENT.publish") as sns_client_mock:
        publish_sns_message(publish_sns_message_input)

    # Then
    assert sns_client_mock.call_args[1] == expected_sns_call
