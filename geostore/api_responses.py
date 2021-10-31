from http import HTTPStatus
from http.client import responses as http_responses
from typing import Callable, Mapping, Union

from jsonschema import ValidationError, validate

from .api_keys import MESSAGE_KEY
from .aws_keys import BODY_KEY, HTTP_METHOD_KEY, STATUS_CODE_KEY
from .types import JsonList, JsonObject


def error_response(code: int, message: str) -> JsonObject:
    return {STATUS_CODE_KEY: code, BODY_KEY: {MESSAGE_KEY: f"{http_responses[code]}: {message}"}}


def success_response(code: int, body: Union[JsonList, JsonObject]) -> JsonObject:
    return {STATUS_CODE_KEY: code, BODY_KEY: body}


def handle_request(
    event: JsonObject, request_handlers: Mapping[str, Callable[[JsonObject], JsonObject]]
) -> JsonObject:
    """Main Lambda entry point."""

    # request validation
    try:
        validate(
            event,
            {
                "type": "object",
                "properties": {
                    HTTP_METHOD_KEY: {"type": "string", "enum": list(request_handlers.keys())},
                    BODY_KEY: {"type": "object"},
                },
                "required": [HTTP_METHOD_KEY, BODY_KEY],
            },
        )
    except ValidationError as err:
        return error_response(HTTPStatus.BAD_REQUEST, err.message)

    method = event[HTTP_METHOD_KEY]
    return request_handlers[method](event[BODY_KEY])
