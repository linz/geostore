from http.client import responses as http_responses
from typing import Callable, Mapping, Union

from jsonschema import ValidationError, validate  # type: ignore[import]

from .types import JsonList, JsonObject


def error_response(code: int, message: str) -> JsonObject:
    return {"statusCode": code, "body": {"message": f"{http_responses[code]}: {message}"}}


def success_response(code: int, body: Union[JsonList, JsonObject]) -> JsonObject:
    return {"statusCode": code, "body": body}


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
                    "httpMethod": {"type": "string", "enum": list(request_handlers.keys())},
                    "body": {"type": "object"},
                },
                "required": ["httpMethod", "body"],
            },
        )
    except ValidationError as err:
        return error_response(400, err.message)

    method = event["httpMethod"]
    return request_handlers[method](event)
