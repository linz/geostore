"""
Dataset-versions endpoint Lambda function.
"""

from jsonschema import ValidationError, validate  # type: ignore[import]

from ..utils import JSON_OBJECT, error_response
from .create import create_dataset_version

REQUEST_SCHEMA = {
    "type": "object",
    "properties": {
        "httpMethod": {"type": "string", "enum": ["POST"]},
        "body": {"type": "object"},
    },
    "required": ["httpMethod", "body"],
}

REQUEST_HANDLERS = {
    "POST": create_dataset_version,
}


def lambda_handler(event: JSON_OBJECT, _context: bytes) -> JSON_OBJECT:
    """Main Lambda entry point."""

    # request validation
    try:
        validate(event, REQUEST_SCHEMA)
        method = event["httpMethod"]
    except ValidationError as err:
        return error_response(400, err.message)

    return REQUEST_HANDLERS[method](event)
