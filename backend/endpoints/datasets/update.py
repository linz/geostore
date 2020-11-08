"""Update dataset function."""

from jsonschema import ValidationError, validate
from pynamodb.exceptions import DoesNotExist

from ..utils import error_response, success_response
from .model import DatasetModel


def update_dataset(payload):
    """PATCH: Update Dataset."""

    BODY_SCHEMA = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "type": {
                "type": "string",
                "enum": ["IMAGE", "RASTER"],
            },
            "title": {"type": "string"},
            "owning_group": {"type": "string"},
        },
        "required": [
            "id",
            "type",
        ],
        "minProperties": 3,
    }

    # request body validation
    req_body = payload["body"]
    try:
        validate(req_body, BODY_SCHEMA)
    except ValidationError as err:
        return error_response(400, err.message)

    # check for duplicate type/title
    if DatasetModel.datasets_tile_idx.count(
        hash_key=f"TYPE#{req_body['type']}",
        range_key_condition=(DatasetModel.title == f"{req_body['title']}"),
    ):
        return error_response(
            409, f"dataset '{req_body['title']}' of type '{req_body['type']}' already exists"
        )

    # get dataset to update
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

    # update dataset
    for attr in DatasetModel.get_attributes():
        if attr not in ("id", "type"):
            if attr in req_body:
                setattr(dataset, attr, req_body[attr])

    dataset.save()
    dataset.refresh(consistent_read=True)

    # return response
    resp_body = {}
    resp_body = dict(dataset)

    resp_body["id"] = dataset.dataset_id
    resp_body["type"] = dataset.dataset_type

    return success_response(200, resp_body)
