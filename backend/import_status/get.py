"""Import Status handler function."""
from http import HTTPStatus
from json import dumps, loads
from typing import TYPE_CHECKING

import boto3
from jsonschema import ValidationError, validate

from ..api_keys import SUCCESS_KEY
from ..api_responses import error_response, success_response
from ..error_response_keys import ERROR_KEY
from ..log import set_up_logging
from ..step_function import get_tasks_status
from ..step_function_keys import (
    DATASET_ID_KEY,
    EXECUTION_ARN_KEY,
    IMPORT_DATASET_KEY,
    STEP_FUNCTION_KEY,
    VALIDATION_KEY,
    VERSION_ID_KEY,
)
from ..types import JsonObject

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_stepfunctions import SFNClient
else:
    # In production we want to avoid depending on a package which has no runtime impact
    SFNClient = object

STEP_FUNCTIONS_CLIENT: SFNClient = boto3.client("stepfunctions")
LOGGER = set_up_logging(__name__)


def get_import_status(body: JsonObject) -> JsonObject:
    LOGGER.debug(dumps({"event": body}))

    try:
        validate(
            body,
            {
                "type": "object",
                "properties": {EXECUTION_ARN_KEY: {"type": "string"}},
                "required": [EXECUTION_ARN_KEY],
            },
        )
    except ValidationError as err:
        LOGGER.warning(dumps({ERROR_KEY: err}, default=str))
        return error_response(HTTPStatus.BAD_REQUEST, err.message)

    step_function_resp = STEP_FUNCTIONS_CLIENT.describe_execution(
        executionArn=body[EXECUTION_ARN_KEY]
    )
    assert "status" in step_function_resp, step_function_resp
    LOGGER.debug(dumps({"step function response": step_function_resp}, default=str))

    step_function_input = loads(step_function_resp["input"])
    step_function_output = loads(step_function_resp.get("output", "{}"))
    step_function_status = step_function_resp["status"]

    dataset_id = step_function_input[DATASET_ID_KEY]
    version_id = step_function_input[VERSION_ID_KEY]
    validation_success = step_function_output.get(VALIDATION_KEY, {}).get(SUCCESS_KEY)
    import_dataset_jobs = step_function_output.get(IMPORT_DATASET_KEY, {})

    tasks_status = get_tasks_status(
        step_function_status, dataset_id, version_id, validation_success, import_dataset_jobs
    )

    response_body = {STEP_FUNCTION_KEY: {"status": step_function_status.title()}, **tasks_status}

    return success_response(HTTPStatus.OK, response_body)
