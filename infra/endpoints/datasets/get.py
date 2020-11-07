"""Get datasets functions."""

from endpoints.datasets.datasets_model import DatasetModel
from endpoints.datasets.utils import error_response, success_response
from jsonschema import ValidationError, validate
from pynamodb.exceptions import DoesNotExist


def get_dataset_single(payload):
    """GET: Get single Dataset."""

    BODY_SCHEMA = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "type": {
                "type": "string",
                "enum": ["IMAGE", "RASTER"],
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
    resp_body = {}
    resp_body = dict(dataset)

    resp_body["id"] = dataset.dataset_id
    resp_body["type"] = dataset.dataset_type

    return success_response(200, resp_body)


def get_dataset_filter(payload):
    """GET: Get Datasets by filter."""

    BODY_SCHEMA = {
        "type": "object",
        "properties": {
            "type": {
                "type": "string",
                "enum": ["IMAGE", "RASTER"],
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
        resp_item = dict(dataset)
        resp_item["id"] = dataset.dataset_id
        resp_item["type"] = dataset.dataset_type
        resp_body.append(resp_item)

    return success_response(200, resp_body)
