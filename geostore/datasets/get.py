"""Get datasets functions."""
from http import HTTPStatus

from jsonschema import ValidationError, validate
from pynamodb.exceptions import DoesNotExist

from ..api_responses import error_response, success_response
from ..datasets_model import datasets_model_with_meta
from ..models import DATASET_ID_PREFIX
from ..step_function_keys import DATASET_ID_SHORT_KEY, TITLE_KEY
from ..types import JsonObject
from .list import list_datasets


def handle_get(body: JsonObject) -> JsonObject:
    if DATASET_ID_SHORT_KEY in body:
        return get_dataset_single(body)

    if TITLE_KEY in body:
        return get_dataset_filter(body)

    if body == {}:
        return list_datasets()

    return error_response(HTTPStatus.BAD_REQUEST, "Unhandled request")


def get_dataset_single(body: JsonObject) -> JsonObject:
    """GET: Get single Dataset."""

    body_schema = {
        "type": "object",
        "properties": {DATASET_ID_SHORT_KEY: {"type": "string"}},
        "required": [DATASET_ID_SHORT_KEY],
    }

    # request body validation
    try:
        validate(body, body_schema)
    except ValidationError as err:
        return error_response(HTTPStatus.BAD_REQUEST, err.message)

    datasets_model_class = datasets_model_with_meta()

    # get dataset
    try:
        dataset = datasets_model_class.get(
            hash_key=f"{DATASET_ID_PREFIX}{body[DATASET_ID_SHORT_KEY]}", consistent_read=True
        )
    except DoesNotExist:
        return error_response(
            HTTPStatus.NOT_FOUND, f"dataset '{body[DATASET_ID_SHORT_KEY]}' does not exist"
        )

    # return response
    resp_body = dataset.as_dict()

    return success_response(HTTPStatus.OK, resp_body)


def get_dataset_filter(body: JsonObject) -> JsonObject:
    """GET: Get Datasets by filter."""

    body_schema = {
        "type": "object",
        "properties": {TITLE_KEY: {"type": "string"}},
        "required": [TITLE_KEY],
    }

    # request body validation
    try:
        validate(body, body_schema)
    except ValidationError as err:
        return error_response(HTTPStatus.BAD_REQUEST, err.message)

    # dataset query by filter
    datasets_model_class = datasets_model_with_meta()
    datasets = datasets_model_class.datasets_title_idx.query(hash_key=body[TITLE_KEY])

    # return response
    resp_body = []
    for dataset in datasets:
        resp_item = dataset.as_dict()
        resp_body.append(resp_item)

    return success_response(HTTPStatus.OK, resp_body)
