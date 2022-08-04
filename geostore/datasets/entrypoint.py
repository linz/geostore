"""
Dataset endpoint Lambda function.
"""
from logging import Logger
from typing import Callable, MutableMapping

from linz_logger import get_log

from ..api_responses import handle_request
from ..logging_keys import LOG_MESSAGE_LAMBDA_START
from ..parameter_store import ParameterName, get_param
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

LOGGER: Logger = get_log()


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    LOGGER.debug(
        LOG_MESSAGE_LAMBDA_START,
        extra={"lambda_input": event, "commit": get_param(ParameterName.GIT_COMMIT)},
    )
    return handle_request(event, REQUEST_HANDLERS)
