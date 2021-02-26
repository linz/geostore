"""
Dataset endpoint Lambda function.
"""
from typing import Callable, MutableMapping

from jsonschema import ValidationError, validate  # type: ignore[import]

from ..utils import JsonObject, error_response
from .create import create_dataset
from .delete import delete_dataset
from .get import handle_get
from .update import update_dataset

REQUEST_SCHEMA = {
    "type": "object",
    "properties": {
        "httpMethod": {"type": "string", "enum": ["GET", "POST", "PATCH", "DELETE"]},
        "body": {"type": "object"},
    },
    "required": ["httpMethod", "body"],
}

# TODO: implement GET response paging
# TODO: allow Dataset delete only if no Dataset Version exists


REQUEST_HANDLERS: MutableMapping[str, Callable[[JsonObject], JsonObject]] = {
    "DELETE": delete_dataset,
    "GET": handle_get,
    "PATCH": update_dataset,
    "POST": create_dataset,
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
