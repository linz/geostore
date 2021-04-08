"""Create dataset function."""

from jsonschema import ValidationError, validate  # type: ignore[import]

from ..api_responses import error_response, success_response
from ..datasets_model import datasets_model_with_meta
from ..types import JsonObject


def create_dataset(payload: JsonObject) -> JsonObject:
    """POST: Create Dataset."""

    body_schema = {
        "type": "object",
        "properties": {"title": {"type": "string"}, "owning_group": {"type": "string"}},
        "required": ["title", "owning_group"],
    }

    # request body validation
    req_body = payload["body"]
    try:
        validate(req_body, body_schema)
    except ValidationError as err:
        return error_response(400, err.message)

    # check for duplicate type/title
    datasets_model_class = datasets_model_with_meta()
    if datasets_model_class.datasets_title_idx.count(hash_key=req_body["title"]):
        return error_response(409, f"dataset '{req_body['title']}' already exists")

    # create dataset
    dataset = datasets_model_class(title=req_body["title"], owning_group=req_body["owning_group"])
    dataset.save()
    dataset.refresh(consistent_read=True)

    # return response
    resp_body = dataset.as_dict()

    return success_response(201, resp_body)
