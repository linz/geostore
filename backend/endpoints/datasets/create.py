"""Create dataset function."""

from endpoints.datasets.datasets_model import DatasetModel
from endpoints.datasets.utils import error_response, success_response
from jsonschema import ValidationError, validate


def create_dataset(payload):
    """POST: Create Dataset."""

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
        "required": ["type", "title", "owning_group"],
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

    # create dataset
    dataset = DatasetModel(
        type=f"TYPE#{req_body['type']}",
        title=req_body["title"],
        owning_group=req_body["owning_group"],
    )
    dataset.save()
    dataset.refresh(consistent_read=True)

    # return response
    resp_body = {}
    resp_body = dict(dataset)

    resp_body["id"] = dataset.dataset_id
    resp_body["type"] = dataset.dataset_type

    return success_response(201, resp_body)
