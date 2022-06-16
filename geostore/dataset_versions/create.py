"""Dataset versions handler function."""
from datetime import datetime
from http import HTTPStatus
from json import dumps
from logging import Logger
from typing import TYPE_CHECKING

import boto3
from jsonschema import ValidationError, validate
from linz_logger import get_log
from pynamodb.exceptions import DoesNotExist
from ulid import from_timestamp

from ..api_responses import error_response, success_response
from ..boto3_config import CONFIG
from ..datasets_model import datasets_model_with_meta, human_readable_ulid
from ..logging_keys import (
    LOG_MESSAGE_LAMBDA_FAILURE,
    LOG_MESSAGE_LAMBDA_START,
    LOG_MESSAGE_STEP_FUNCTION_RESPONSE,
)
from ..models import DATASET_ID_PREFIX
from ..parameter_store import ParameterName, get_param
from ..step_function_keys import (
    CURRENT_VERSION_ID_KEY,
    DATASET_ID_KEY,
    DATASET_ID_SHORT_KEY,
    DATASET_PREFIX_KEY,
    EXECUTION_ARN_KEY,
    METADATA_URL_KEY,
    NEW_VERSION_ID_KEY,
    NOW_KEY,
    S3_ROLE_ARN_KEY,
)
from ..types import JsonObject

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_stepfunctions import SFNClient
else:
    # In production we want to avoid depending on a package which has no runtime impact
    SFNClient = object  # pragma: no mutate

STEP_FUNCTIONS_CLIENT: SFNClient = boto3.client("stepfunctions", config=CONFIG)
LOGGER: Logger = get_log()


def create_dataset_version(body: JsonObject) -> JsonObject:

    LOGGER.debug(LOG_MESSAGE_LAMBDA_START, extra={"lambda_input": body})

    body_schema = {
        "type": "object",
        "properties": {
            DATASET_ID_SHORT_KEY: {"type": "string"},
            METADATA_URL_KEY: {"type": "string"},
            NOW_KEY: {"type": "string", "format": "date-time"},
            S3_ROLE_ARN_KEY: {"type": "string"},
        },
        "required": [DATASET_ID_SHORT_KEY, METADATA_URL_KEY, S3_ROLE_ARN_KEY],
    }

    # validate input
    try:
        validate(body, body_schema)
    except ValidationError as err:
        LOGGER.warning(LOG_MESSAGE_LAMBDA_FAILURE, extra={"error": err.message})
        return error_response(HTTPStatus.BAD_REQUEST, err.message)

    datasets_model_class = datasets_model_with_meta()

    # validate dataset exists
    try:
        dataset = datasets_model_class.get(
            hash_key=f"{DATASET_ID_PREFIX}{body[DATASET_ID_SHORT_KEY]}", consistent_read=True
        )
    except DoesNotExist as err:
        LOGGER.warning(LOG_MESSAGE_LAMBDA_FAILURE, extra={"error": err.msg})
        return error_response(
            HTTPStatus.NOT_FOUND, f"dataset '{body[DATASET_ID_SHORT_KEY]}' could not be found"
        )

    now = datetime.fromisoformat(body.get(NOW_KEY, datetime.utcnow().isoformat()))
    dataset_version_id = human_readable_ulid(from_timestamp(now))
    current_dataset_version = dataset.current_dataset_version or "None"

    # execute step function
    step_functions_input = {
        DATASET_ID_KEY: dataset.dataset_id,
        DATASET_PREFIX_KEY: dataset.title,
        NEW_VERSION_ID_KEY: dataset_version_id,
        CURRENT_VERSION_ID_KEY: current_dataset_version,
        METADATA_URL_KEY: body[METADATA_URL_KEY],
        S3_ROLE_ARN_KEY: body[S3_ROLE_ARN_KEY],
    }
    state_machine_arn = get_param(
        ParameterName.PROCESSING_DATASET_VERSION_CREATION_STEP_FUNCTION_ARN
    )

    step_functions_response = STEP_FUNCTIONS_CLIENT.start_execution(
        stateMachineArn=state_machine_arn,
        name=dataset_version_id,
        input=dumps(step_functions_input),
    )

    LOGGER.debug(LOG_MESSAGE_STEP_FUNCTION_RESPONSE, extra={"response": step_functions_response})

    # return arn of executing process
    return success_response(
        HTTPStatus.CREATED,
        {
            NEW_VERSION_ID_KEY: dataset_version_id,
            EXECUTION_ARN_KEY: step_functions_response["executionArn"],
        },
    )
