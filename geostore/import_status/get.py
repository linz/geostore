"""Import Status handler function."""
from http import HTTPStatus
from json import dumps

from jsonschema import ValidationError, validate
from linz_logger import get_log

from ..api_keys import EVENT_KEY
from ..api_responses import error_response, success_response
from ..error_response_keys import ERROR_KEY
from ..step_function import get_import_status_given_arn
from ..step_function_keys import EXECUTION_ARN_KEY
from ..types import JsonObject

LOGGER = get_log()


def get_import_status(body: JsonObject) -> JsonObject:
    LOGGER.debug(dumps({EVENT_KEY: body}))

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

    response_body = get_import_status_given_arn(body[EXECUTION_ARN_KEY])

    return success_response(HTTPStatus.OK, response_body)
