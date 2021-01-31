"""Get datasets functions."""

from jsonschema import ValidationError, validate  # type: ignore[import]
from pynamodb.exceptions import DoesNotExist

from ..utils import error_response, success_response
from .common import DATASET_TYPES
from .list import list_datasets
from .model import DatasetModel
from .serializer import serialize_dataset


def handle_get(event):
    if "id" in event["body"] and "type" in event["body"]:
        return get_dataset_single(event)

    if "title" in event["body"] or "owning_group" in event["body"]:
        return get_dataset_filter(event)

    if event["body"] == {}:
        return list_datasets()

    return error_response(400, "Unhandled request")


def get_dataset_single(payload):
    """GET: Get single Dataset."""

    BODY_SCHEMA = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "type": {
                "type": "string",
                "enum": DATASET_TYPES,
            },
        },
        "required": ["id", "type"],
    }

    # request body validation
    req_body = payload["body"]
    try:
        validate(req_body, BODY_SCHEMA)
    except ValidationError as err:
        return error_response(400, err.message)

    # get dataset
    try:
        dataset = DatasetModel.get(
            hash_key=f"DATASET#{req_body['id']}",
            range_key=f"TYPE#{req_body['type']}",
            consistent_read=True,
        )
    except DoesNotExist:
        return error_response(
            404, f"dataset '{req_body['id']}' of type '{req_body['type']}' does not exist"
        )

    # return response
    resp_body = serialize_dataset(dataset)

    return success_response(200, resp_body)


def get_dataset_filter(payload):
    """GET: Get Datasets by filter."""

    BODY_SCHEMA = {
        "type": "object",
        "properties": {
            "type": {
                "type": "string",
                "enum": DATASET_TYPES,
            },
            "title": {"type": "string"},
            "owning_group": {"type": "string"},
        },
        "required": ["type"],
        "minProperties": 2,
        "maxProperties": 2,
    }

    # request body validation
    req_body = payload["body"]
    try:
        validate(req_body, BODY_SCHEMA)
    except ValidationError as err:
        return error_response(400, err.message)

    # dataset query by filter
    if "title" in req_body:
        datasets = DatasetModel.datasets_tile_idx.query(
            hash_key=f"TYPE#{req_body['type']}",
            range_key_condition=DatasetModel.title == f"{req_body['title']}",
        )

    if "owning_group" in req_body:
        datasets = DatasetModel.datasets_owning_group_idx.query(
            hash_key=f"TYPE#{req_body['type']}",
            range_key_condition=DatasetModel.owning_group == f"{req_body['owning_group']}",
        )

    # return response
    resp_body = []
    for dataset in datasets:
        resp_item = serialize_dataset(dataset)
        resp_body.append(resp_item)

    return success_response(200, resp_body)
