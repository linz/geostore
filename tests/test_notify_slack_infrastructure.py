from datetime import datetime, timezone
from http import HTTPStatus
from json import dumps, load
from os import environ
from unittest.mock import patch

from mypy_boto3_events import EventBridgeClient
from mypy_boto3_lambda import LambdaClient
from pytest import mark

from backend.api_responses import STATUS_CODE_KEY
from backend.aws_keys import AWS_DEFAULT_REGION_KEY
from backend.resources import ResourceName
from backend.step_function_keys import (
    DATASET_ID_KEY,
    DATASET_PREFIX_KEY,
    INPUT_KEY,
    JOB_STATUS_FAILED,
    OUTPUT_KEY,
    STATUS_KEY,
)

from .aws_profile_utils import any_region_name
from .stac_generators import any_dataset_id, any_dataset_prefix

with patch.dict(
    environ, {AWS_DEFAULT_REGION_KEY: environ.get(AWS_DEFAULT_REGION_KEY, any_region_name())}
):
    from backend.notify_status_update.task import EVENT_DETAIL_KEY

STEP_FUNCTION_START_MILLISECOND_TIMESTAMP = round(
    datetime(
        2001, 2, 3, hour=4, minute=5, second=6, microsecond=789876, tzinfo=timezone.utc
    ).timestamp()
    * 1000
)
STEP_FUNCTION_STOP_MILLISECOND_TIMESTAMP = STEP_FUNCTION_START_MILLISECOND_TIMESTAMP + 10


@mark.infrastructure
def should_launch_notify_slack_endpoint_lambda_function(
    lambda_client: LambdaClient, events_client: EventBridgeClient
) -> None:
    notify_status_lambda_arn = events_client.list_targets_by_rule(
        Rule=ResourceName.CLOUDWATCH_RULE_NAME.value
    )["Targets"][0]["Arn"]

    # When
    body = {
        EVENT_DETAIL_KEY: {
            STATUS_KEY: JOB_STATUS_FAILED,
            INPUT_KEY: dumps(
                {
                    DATASET_ID_KEY: any_dataset_id(),
                    DATASET_PREFIX_KEY: any_dataset_prefix(),
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
