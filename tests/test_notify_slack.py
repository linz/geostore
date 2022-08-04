from datetime import datetime, timezone
from http import HTTPStatus
from json import dumps, load
from os import environ
from unittest.mock import MagicMock, patch

from mypy_boto3_events import EventBridgeClient
from mypy_boto3_lambda import LambdaClient
from mypy_boto3_sns.type_defs import MessageAttributeValueTypeDef
from pytest import mark
from pytest_subtests import SubTests

from geostore.aws_keys import STATUS_CODE_KEY
from geostore.aws_message_attributes import DATA_TYPE_STRING
from geostore.logging_keys import LOG_MESSAGE_LAMBDA_START
from geostore.notify_status_update.task import (
    EVENT_DETAIL_KEY,
    MESSAGE_ATTRIBUTE_DATASET_TITLE_KEY,
    MESSAGE_ATTRIBUTE_STATUS_KEY,
    SLACK_URL_ENV_NAME,
    STEP_FUNCTION_ARN_KEY,
    STEP_FUNCTION_STARTDATE_KEY,
    STEP_FUNCTION_STOPDATE_KEY,
    WEBHOOK_MESSAGE_BLOCKS_KEY,
    lambda_handler,
    publish_sns_message,
)
from geostore.parameter_store import ParameterName, get_param
from geostore.resources import Resource
from geostore.step_function import Outcome
from geostore.step_function_keys import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_KEY,
    DATASET_TITLE_KEY,
    ERRORS_KEY,
    INPUT_KEY,
    JOB_STATUS_FAILED,
    JOB_STATUS_RUNNING,
    JOB_STATUS_SUCCEEDED,
    METADATA_UPLOAD_KEY,
    NEW_VERSION_ID_KEY,
    NEW_VERSION_S3_LOCATION,
    OUTPUT_KEY,
    STATUS_KEY,
    STEP_FUNCTION_KEY,
    UPDATE_DATASET_KEY,
    UPLOAD_STATUS_KEY,
    VALIDATION_KEY,
)

from .aws_utils import any_arn_formatted_string, any_lambda_context, any_s3_url
from .general_generators import any_https_url
from .stac_generators import any_dataset_id, any_dataset_title, any_dataset_version_id

STEP_FUNCTION_START_MILLISECOND_TIMESTAMP = round(
    datetime(
        2001, 2, 3, hour=4, minute=5, second=6, microsecond=789876, tzinfo=timezone.utc
    ).timestamp()
    * 1000
)
STEP_FUNCTION_STOP_MILLISECOND_TIMESTAMP = STEP_FUNCTION_START_MILLISECOND_TIMESTAMP + 10


@patch("geostore.notify_status_update.task.WebhookClient.send")
@patch("geostore.notify_status_update.task.get_import_status_given_arn")
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
        "geostore.notify_status_update.task.publish_sns_message"
    ):
        # When
        notify_status_update_input = {
            EVENT_DETAIL_KEY: {
                STATUS_KEY: JOB_STATUS_SUCCEEDED,
                STEP_FUNCTION_ARN_KEY: any_arn_formatted_string(),
                INPUT_KEY: dumps(
                    {
                        DATASET_ID_KEY: any_dataset_id(),
                        DATASET_TITLE_KEY: any_dataset_title(),
                        NEW_VERSION_ID_KEY: any_dataset_version_id(),
                    }
                ),
                OUTPUT_KEY: dumps(
                    {
                        UPLOAD_STATUS_KEY: {
                            VALIDATION_KEY: "",
                            ASSET_UPLOAD_KEY: "",
                            METADATA_UPLOAD_KEY: "",
                        },
                        UPDATE_DATASET_KEY: {NEW_VERSION_S3_LOCATION: any_s3_url()},
                    }
                ),
                STEP_FUNCTION_STARTDATE_KEY: STEP_FUNCTION_START_MILLISECOND_TIMESTAMP,
                STEP_FUNCTION_STOPDATE_KEY: STEP_FUNCTION_STOP_MILLISECOND_TIMESTAMP,
            }
        }

        lambda_handler(notify_status_update_input, any_lambda_context())

        # Then assert there is 15 slack_sdk message 'blocks' sent to webhook url
        webhook_client_mock.assert_called_once()
        assert len(webhook_client_mock.call_args[1][WEBHOOK_MESSAGE_BLOCKS_KEY]) == 15


@patch("geostore.notify_status_update.task.WebhookClient.send")
def should_not_notify_slack_when_step_function_running(webhook_client_mock: MagicMock) -> None:
    # Given

    webhook_client_mock.return_value.status_code = HTTPStatus.OK

    mock_slack_url = any_https_url()

    with patch.dict(environ, {SLACK_URL_ENV_NAME: mock_slack_url}), patch(
        "geostore.notify_status_update.task.publish_sns_message"
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


@patch("geostore.notify_status_update.task.WebhookClient.send")
@patch("geostore.notify_status_update.task.get_import_status_given_arn")
def should_notify_slack_when_step_function_failed(
    step_func_status_mock: MagicMock, webhook_client_mock: MagicMock
) -> None:
    # Given

    webhook_client_mock.return_value.status_code = HTTPStatus.OK
    mock_slack_url = any_https_url()

    step_func_status_mock.return_value = {
        STEP_FUNCTION_KEY: {STATUS_KEY: JOB_STATUS_FAILED},
        VALIDATION_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
        METADATA_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
        ASSET_UPLOAD_KEY: {STATUS_KEY: Outcome.SKIPPED.value, ERRORS_KEY: []},
    }

    with patch.dict(environ, {SLACK_URL_ENV_NAME: mock_slack_url}), patch(
        "geostore.notify_status_update.task.publish_sns_message"
    ):
        # When
        notify_status_update_input = {
            EVENT_DETAIL_KEY: {
                STATUS_KEY: JOB_STATUS_FAILED,
                STEP_FUNCTION_ARN_KEY: any_arn_formatted_string(),
                INPUT_KEY: dumps(
                    {
                        DATASET_ID_KEY: any_dataset_id(),
                        DATASET_TITLE_KEY: any_dataset_title(),
                        NEW_VERSION_ID_KEY: any_dataset_version_id(),
                    }
                ),
                STEP_FUNCTION_STARTDATE_KEY: STEP_FUNCTION_START_MILLISECOND_TIMESTAMP,
                STEP_FUNCTION_STOPDATE_KEY: STEP_FUNCTION_STOP_MILLISECOND_TIMESTAMP,
            },
            OUTPUT_KEY: None,
        }

        lambda_handler(notify_status_update_input, any_lambda_context())

        # Then assert there is 13 slack_sdk message 'blocks' sent to webhook url
        webhook_client_mock.assert_called_once()
        assert len(webhook_client_mock.call_args[1][WEBHOOK_MESSAGE_BLOCKS_KEY]) == 13


@patch("geostore.notify_status_update.task.WebhookClient.send")
def should_log_and_not_post_to_slack_when_url_not_set(
    webhook_client_mock: MagicMock, subtests: SubTests
) -> None:
    # Given
    with patch("geostore.notify_status_update.task.publish_sns_message"), patch(
        "geostore.notify_status_update.task.LOGGER.debug"
    ) as logger_mock:
        # When
        lambda_handler({}, any_lambda_context())

    # Then
    with subtests.test("no slack message"):
        assert not webhook_client_mock.called

    with subtests.test("log created"):
        logger_mock.assert_any_call(
            LOG_MESSAGE_LAMBDA_START,
            extra={"lambda_input": {}, "git_commit": get_param(ParameterName.GIT_COMMIT)},
        )


@patch("geostore.notify_status_update.task.get_param")
def should_publish_sns_message(get_param_mock: MagicMock) -> None:
    # Given
    get_param_mock.return_value = topic_arn = any_arn_formatted_string()
    dataset_title = any_dataset_title()
    publish_sns_message_input = {
        EVENT_DETAIL_KEY: {
            STATUS_KEY: JOB_STATUS_SUCCEEDED,
            INPUT_KEY: dumps({DATASET_TITLE_KEY: dataset_title}),
        }
    }

    expected_sns_call = {
        "TopicArn": topic_arn,
        "Message": dumps(publish_sns_message_input),
        "MessageAttributes": {
            MESSAGE_ATTRIBUTE_DATASET_TITLE_KEY: MessageAttributeValueTypeDef(
                DataType=DATA_TYPE_STRING, StringValue=dataset_title
            ),
            MESSAGE_ATTRIBUTE_STATUS_KEY: MessageAttributeValueTypeDef(
                DataType=DATA_TYPE_STRING, StringValue=JOB_STATUS_SUCCEEDED
            ),
        },
    }

    # When
    with patch("geostore.notify_status_update.task.SNS_CLIENT.publish") as sns_client_mock:
        publish_sns_message(publish_sns_message_input)

    # Then
    assert sns_client_mock.call_args[1] == expected_sns_call


@mark.infrastructure
def should_launch_notify_slack_endpoint_lambda_function(
    lambda_client: LambdaClient, events_client: EventBridgeClient
) -> None:

    notify_status_lambda_arn = events_client.list_targets_by_rule(
        Rule=Resource.CLOUDWATCH_RULE_NAME.resource_name
    )["Targets"][0]["Arn"]

    # When
    body = {
        EVENT_DETAIL_KEY: {
            STATUS_KEY: JOB_STATUS_FAILED,
            INPUT_KEY: dumps(
                {
                    DATASET_ID_KEY: any_dataset_id(),
                    DATASET_TITLE_KEY: any_dataset_title(),
                }
            ),
        },
        OUTPUT_KEY: None,
    }

    resp = load(
        lambda_client.invoke(
            FunctionName=notify_status_lambda_arn,
            Payload=dumps(body).encode(),
        )["Payload"]
    )

    assert resp.get(STATUS_CODE_KEY) == HTTPStatus.OK, resp
