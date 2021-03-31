"""Update dataset function."""

from jsonschema import ValidationError, validate  # type: ignore[import]
from pynamodb.exceptions import DoesNotExist

from ..api_responses import error_response, success_response
from ..dataset import DATASET_TYPES
from ..datasets_model import DatasetsModel
from ..types import JsonObject


def update_dataset(payload: JsonObject) -> JsonObject:
    """PATCH: Update Dataset."""

    body_schema = {
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
        validate(req_body, body_schema)
    except ValidationError as err:
        return error_response(400, err.message)

    # check for duplicate type/title
    if DatasetsModel.datasets_title_idx.count(
        hash_key=f"TYPE#{req_body['type']}",
        range_key_condition=(DatasetsModel.title == req_body["title"]),
    ):
        return error_response(
            409, f"dataset '{req_body['title']}' of type '{req_body['type']}' already exists"
        )

    # get dataset to update
    try:
        dataset = DatasetsModel.get(
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
    resp_body = dataset.as_dict()

    return success_response(200, resp_body)


def update_dataset_attributes(dataset: DatasetsModel, req_body: JsonObject) -> None:
    for attr in DatasetsModel.get_attributes():
        if attr in req_body and attr not in ("id", "type"):
            setattr(dataset, attr, req_body[attr])
