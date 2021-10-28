"""
Dataset-versions endpoint Lambda function.
"""
from typing import Callable, Mapping

from ..api_responses import handle_request
from ..types import JsonObject
from .get import get_import_status

REQUEST_HANDLERS: Mapping[str, Callable[[JsonObject], JsonObject]] = {
    "GET": get_import_status,
}


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    return handle_request(event, REQUEST_HANDLERS)
