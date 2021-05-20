"""
Dataset endpoint Lambda function.
"""
from typing import Callable, MutableMapping

from ..api_responses import handle_request
from ..types import JsonObject
from .create import create_dataset
from .delete import delete_dataset
from .get import handle_get
from .update import update_dataset

REQUEST_HANDLERS: MutableMapping[str, Callable[[JsonObject], JsonObject]] = {
    "DELETE": delete_dataset,
    "GET": handle_get,
    "PATCH": update_dataset,
    "POST": create_dataset,
}


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    return handle_request(event, REQUEST_HANDLERS)
