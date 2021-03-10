"""
Dataset-versions endpoint Lambda function.
"""
from typing import Callable, MutableMapping

from ..api_responses import handle_request
from ..types import JsonObject
from .create import create_dataset_version

REQUEST_HANDLERS: MutableMapping[str, Callable[[JsonObject], JsonObject]] = {
    "POST": create_dataset_version,
}


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    return handle_request(event, REQUEST_HANDLERS)
