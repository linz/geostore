"""Update dataset function."""

from jsonschema import ValidationError, validate  # type: ignore[import]
from pynamodb.exceptions import DoesNotExist

from ..model import DatasetModel
from ..utils import DATASET_TYPES, JSON_OBJECT, error_response, success_response


def update_dataset(payload: JSON_OBJECT) -> JSON_OBJECT:
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
    update_dataset_attributes(dataset, req_body)
    dataset.save()
    dataset.refresh(consistent_read=True)

    # return response
    resp_body = dataset.serialize()

    return success_response(200, resp_body)


def update_dataset_attributes(dataset: DatasetModel, req_body: JSON_OBJECT) -> None:
    for attr in DatasetModel.get_attributes():
        if attr in req_body and attr not in ("id", "type"):
            setattr(dataset, attr, req_body[attr])
