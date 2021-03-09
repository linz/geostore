"""
Dataset-versions endpoint Lambda function.
"""
from typing import Callable, MutableMapping

from jsonschema import ValidationError, validate  # type: ignore[import]

from ..api_responses import JsonObject, error_response
from .create import create_dataset_version

REQUEST_SCHEMA = {
    "type": "object",
    "properties": {
        "httpMethod": {"type": "string", "enum": ["POST"]},
        "body": {"type": "object"},
    },
    "required": ["httpMethod", "body"],
}

REQUEST_HANDLERS: MutableMapping[str, Callable[[JsonObject], JsonObject]] = {
    "POST": create_dataset_version,
}


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    """Main Lambda entry point."""

    # request validation
    try:
        validate(event, REQUEST_SCHEMA)
        method = event["httpMethod"]
    except ValidationError as err:
        return error_response(400, err.message)

    return REQUEST_HANDLERS[method](event)
