"""Update dataset function."""

from jsonschema import ValidationError, validate  # type: ignore[import]
from pynamodb.exceptions import DoesNotExist

from ..api_responses import error_response, success_response
from ..datasets_model import DatasetsModelBase, datasets_model_with_meta
from ..types import JsonObject


def update_dataset(payload: JsonObject) -> JsonObject:
    """PATCH: Update Dataset."""

    body_schema = {
        "type": "object",
        "properties": {"id": {"type": "string"}, "title": {"type": "string"}},
        "required": ["id", "title"],
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

    # get dataset to update
    try:
        dataset = datasets_model_class.get(
            hash_key=f"DATASET#{req_body['id']}", consistent_read=True
        )
    except DoesNotExist:
        return error_response(404, f"dataset '{req_body['id']}' does not exist")

    # update dataset
    update_dataset_attributes(dataset, req_body)
    dataset.save()
    dataset.refresh(consistent_read=True)

    # return response
    resp_body = dataset.as_dict()

    return success_response(200, resp_body)


def update_dataset_attributes(dataset: DatasetsModelBase, req_body: JsonObject) -> None:
    for attr in DatasetsModelBase.get_attributes():
        if attr in req_body and attr != "id":
            setattr(dataset, attr, req_body[attr])
