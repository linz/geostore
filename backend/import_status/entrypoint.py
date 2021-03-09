"""
Dataset-versions endpoint Lambda function.
"""
from typing import Callable, Mapping

from jsonschema import ValidationError, validate  # type: ignore[import]

from ..api_responses import JsonObject, error_response
from .get import get_import_status

REQUEST_SCHEMA = {
    "type": "object",
    "properties": {
        "httpMethod": {"type": "string", "enum": ["GET"]},
        "body": {"type": "object"},
    },
    "required": ["httpMethod", "body"],
}

REQUEST_HANDLERS: Mapping[str, Callable[[JsonObject], JsonObject]] = {
    "GET": get_import_status,
}


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    """Main Lambda entry point."""

    # request validation
    try:
        validate(event, REQUEST_SCHEMA)
    except ValidationError as err:
        return error_response(400, err.message)

    method = event["httpMethod"]
    return REQUEST_HANDLERS[method](event)
