"""Import Status handler function."""
from http import HTTPStatus

from jsonschema import ValidationError, validate
from linz_logger import get_log

from ..api_responses import error_response, success_response
from ..logging_keys import LOG_MESSAGE_LAMBDA_FAILURE, LOG_MESSAGE_LAMBDA_START
from ..step_function import get_import_status_given_arn
from ..step_function_keys import EXECUTION_ARN_KEY
from ..types import JsonObject

LOGGER = get_log()


def get_import_status(body: JsonObject) -> JsonObject:
    LOGGER.debug(LOG_MESSAGE_LAMBDA_START, lambda_input=body)

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
        LOGGER.warning(LOG_MESSAGE_LAMBDA_FAILURE, error=err.message)
        return error_response(HTTPStatus.BAD_REQUEST, err.message)

    response_body = get_import_status_given_arn(body[EXECUTION_ARN_KEY])

    return success_response(HTTPStatus.OK, response_body)
