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
from backend.notify_status_update.task import (
    EVENT_DETAIL_KEY,
    MESSAGE_ATTRIBUTE_DATASET_KEY,
    MESSAGE_ATTRIBUTE_STATUS_KEY,
    SLACK_URL_ENV_NAME,
    STEP_FUNCTION_ARN_KEY,
    STEP_FUNCTION_INPUT_KEY,
    STEP_FUNCTION_OUTPUT_KEY,
    STEP_FUNCTION_STARTDATE_KEY,
    STEP_FUNCTION_STOPDATE_KEY,
    WEBHOOK_MESSAGE_BLOCKS_KEY,
    lambda_handler,
    publish_sns_message,
)
from backend.step_function import Outcome
from backend.step_function_keys import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_KEY,
    DATASET_PREFIX_KEY,
    ERRORS_KEY,
    JOB_STATUS_FAILED,
    JOB_STATUS_RUNNING,
    JOB_STATUS_SUCCEEDED,
    METADATA_UPLOAD_KEY,
    NEW_VERSION_S3_LOCATION,
    STATUS_KEY,
    STEP_FUNCTION_KEY,
    UPDATE_DATASET_KEY,
    UPLOAD_STATUS_KEY,
    VALIDATION_KEY,
    VERSION_ID_KEY,
)

from .aws_utils import any_arn_formatted_string, any_lambda_context, any_s3_url
from .general_generators import any_https_url
from .stac_generators import any_dataset_id, any_dataset_prefix, any_dataset_version_id


@patch("backend.notify_status_update.task.WebhookClient.send")
@patch("backend.notify_status_update.task.get_import_status_given_arn")
def should_notify_slack_with_finished_details_when_url_set(
    step_func_status_mock: MagicMock, webhook_client_mock: MagicMock
) -> None:
    # Given

    webhook_client_mock.return_value.status_code = HTTPStatus.OK

    step_func_status_mock.return_value = {
        STEP_FUNCTION_KEY: {STATUS_KEY: JOB_STATUS_SUCCEEDED},
        VALIDATION_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
        METADATA_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
        ASSET_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
    }

    mock_slack_url = any_https_url()

    with patch.dict(environ, {SLACK_URL_ENV_NAME: mock_slack_url}), patch(
        "backend.notify_status_update.task.publish_sns_message"
    ):
        now_ts = datetime.now()
        # When
        notify_status_update_input = {
            EVENT_DETAIL_KEY: {
                STATUS_KEY: JOB_STATUS_SUCCEEDED,
                STEP_FUNCTION_ARN_KEY: any_arn_formatted_string(),
                STEP_FUNCTION_INPUT_KEY: dumps(
                    {
                        DATASET_ID_KEY: any_dataset_id(),
                        DATASET_PREFIX_KEY: any_dataset_prefix(),
                        VERSION_ID_KEY: any_dataset_version_id(),
                    }
                ),
                STEP_FUNCTION_OUTPUT_KEY: dumps(
                    {
                        UPLOAD_STATUS_KEY: {
                            VALIDATION_KEY: "",
                            ASSET_UPLOAD_KEY: "",
                            METADATA_UPLOAD_KEY: "",
                        },
                        UPDATE_DATASET_KEY: {NEW_VERSION_S3_LOCATION: any_s3_url()},
                    }
                ),
                STEP_FUNCTION_STARTDATE_KEY: round(
                    (now_ts - timedelta(seconds=10)).timestamp() * 1000
                ),
                STEP_FUNCTION_STOPDATE_KEY: round(now_ts.timestamp() * 1000),
            }
        }

        lambda_handler(notify_status_update_input, any_lambda_context())

        # Then
        webhook_client_mock.assert_called_once()
        assert len(webhook_client_mock.call_args[1][WEBHOOK_MESSAGE_BLOCKS_KEY]) == 11


@patch("backend.notify_status_update.task.WebhookClient.send")
def should_not_notify_slack_when_step_function_running(webhook_client_mock: MagicMock) -> None:
    # Given

    webhook_client_mock.return_value.status_code = HTTPStatus.OK

    mock_slack_url = any_https_url()

    with patch.dict(environ, {SLACK_URL_ENV_NAME: mock_slack_url}), patch(
        "backend.notify_status_update.task.publish_sns_message"
    ):
        # When
        notify_status_update_input = {
            EVENT_DETAIL_KEY: {
                STATUS_KEY: JOB_STATUS_RUNNING,
                STEP_FUNCTION_STOPDATE_KEY: None,
            }
        }

        lambda_handler(notify_status_update_input, any_lambda_context())

        # Then
        webhook_client_mock.assert_not_called()


@patch("backend.notify_status_update.task.WebhookClient.send")
@patch("backend.notify_status_update.task.get_import_status_given_arn")
def should_not_notify_slack_when_step_function_failed(
    step_func_status_mock: MagicMock, webhook_client_mock: MagicMock
) -> None:
    # Given

    webhook_client_mock.return_value.status_code = HTTPStatus.OK
    mock_slack_url = any_https_url()
    now_ts = datetime.now()

    step_func_status_mock.return_value = {
        STEP_FUNCTION_KEY: {STATUS_KEY: JOB_STATUS_FAILED},
        VALIDATION_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
        METADATA_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
        ASSET_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
    }

    with patch.dict(environ, {SLACK_URL_ENV_NAME: mock_slack_url}), patch(
        "backend.notify_status_update.task.publish_sns_message"
    ):
        # When
        notify_status_update_input = {
            EVENT_DETAIL_KEY: {
                STATUS_KEY: JOB_STATUS_FAILED,
                STEP_FUNCTION_ARN_KEY: any_arn_formatted_string(),
                STEP_FUNCTION_INPUT_KEY: dumps(
                    {
                        DATASET_ID_KEY: any_dataset_id(),
                        DATASET_PREFIX_KEY: any_dataset_prefix(),
                        VERSION_ID_KEY: any_dataset_version_id(),
                    }
                ),
                STEP_FUNCTION_STARTDATE_KEY: round(
                    (now_ts - timedelta(seconds=10)).timestamp() * 1000
                ),
                STEP_FUNCTION_STOPDATE_KEY: round(now_ts.timestamp() * 1000),
            },
            STEP_FUNCTION_OUTPUT_KEY: None,
        }

        lambda_handler(notify_status_update_input, any_lambda_context())

        # Then
        webhook_client_mock.assert_called_once()
        assert len(webhook_client_mock.call_args[1][WEBHOOK_MESSAGE_BLOCKS_KEY]) == 9


@patch("backend.notify_status_update.task.WebhookClient.send")
def should_log_and_not_post_to_slack_when_url_not_set(
    webhook_client_mock: MagicMock, subtests: SubTests
) -> None:
    # Given
    logger = getLogger("backend.notify_status_update.task")

    with patch("backend.notify_status_update.task.publish_sns_message"), patch.object(
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


@patch("backend.notify_status_update.task.get_param")
def should_publish_sns_message(get_param_mock: MagicMock) -> None:
    # Given
    get_param_mock.return_value = topic_arn = any_arn_formatted_string()
    dataset_prefix = any_dataset_prefix()
    publish_sns_message_input = {
        EVENT_DETAIL_KEY: {
            STATUS_KEY: JOB_STATUS_SUCCEEDED,
            STEP_FUNCTION_INPUT_KEY: dumps(
                {
                    DATASET_PREFIX_KEY: dataset_prefix,
                }
            ),
        }
    }

    expected_sns_call = {
        "TopicArn": topic_arn,
        "Message": dumps(publish_sns_message_input),
        "MessageAttributes": {
            MESSAGE_ATTRIBUTE_DATASET_KEY: MessageAttributeValueTypeDef(
                DataType=DATA_TYPE_STRING, StringValue=dataset_prefix
            ),
            MESSAGE_ATTRIBUTE_STATUS_KEY: MessageAttributeValueTypeDef(
                DataType=DATA_TYPE_STRING, StringValue=JOB_STATUS_SUCCEEDED
            ),
        },
    }

    # When
    with patch("backend.notify_status_update.task.SNS_CLIENT.publish") as sns_client_mock:
        publish_sns_message(publish_sns_message_input)

    # Then
    assert sns_client_mock.call_args[1] == expected_sns_call
