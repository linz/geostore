"""Create dataset function."""

from jsonschema import ValidationError, validate  # type: ignore[import]

from ..api_responses import error_response, success_response
from ..dataset import DATASET_TYPES
from ..datasets_model import DatasetsModel
from ..types import JsonObject


def create_dataset(payload: JsonObject) -> JsonObject:
    """POST: Create Dataset."""

    body_schema = {
        "type": "object",
        "properties": {
            "type": {
                "type": "string",
                "enum": DATASET_TYPES,
            },
            "title": {"type": "string"},
            "owning_group": {"type": "string"},
        },
        "required": ["type", "title", "owning_group"],
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

    # create dataset
    dataset = DatasetsModel(
        type=f"TYPE#{req_body['type']}",
        title=req_body["title"],
        owning_group=req_body["owning_group"],
    )
    dataset.save()
    dataset.refresh(consistent_read=True)

    # return response
    resp_body = dataset.as_dict()

    return success_response(201, resp_body)
