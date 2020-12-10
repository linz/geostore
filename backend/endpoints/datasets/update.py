"""Update dataset function."""

from jsonschema import ValidationError, validate
from pynamodb.exceptions import DoesNotExist

from ..utils import error_response, success_response
from .common import DATASET_TYPES
from .model import DatasetModel
from .serializer import serialize_dataset


def update_dataset(payload):
    """PATCH: Update Dataset."""

    BODY_SCHEMA = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "type": {
                "type": "string",
                "enum": DATASET_TYPES,
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
    for key, value in body_attributes_in_model(req_body).items():
        setattr(dataset, key, value)

    dataset.save()
    dataset.refresh(consistent_read=True)

    # return response
    resp_body = serialize_dataset(dataset)

    return success_response(200, resp_body)


def body_attributes_in_model(body):
    result = {}
    for attr in DatasetModel.get_attributes():
        if attr not in ("id", "type"):
            if attr in body:
                result[attr] = body[attr]

    return result
