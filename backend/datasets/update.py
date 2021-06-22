"""Update dataset function."""
from http import HTTPStatus

from jsonschema import ValidationError, validate
from pynamodb.exceptions import DoesNotExist

from ..api_responses import error_response, success_response
from ..datasets_model import DatasetsModelBase, datasets_model_with_meta
from ..models import DATASET_ID_PREFIX
from ..step_function_keys import DATASET_ID_SHORT_KEY, TITLE_KEY
from ..types import JsonObject


def update_dataset(body: JsonObject) -> JsonObject:
    """PATCH: Update Dataset."""

    body_schema = {
        "type": "object",
        "properties": {DATASET_ID_SHORT_KEY: {"type": "string"}, TITLE_KEY: {"type": "string"}},
        "required": [DATASET_ID_SHORT_KEY, TITLE_KEY],
    }

    # request body validation
    try:
        validate(body, body_schema)
    except ValidationError as err:
        return error_response(HTTPStatus.BAD_REQUEST, err.message)

    # check for duplicate type/title
    datasets_model_class = datasets_model_with_meta()
    dataset_title = body[TITLE_KEY]
    if datasets_model_class.datasets_title_idx.count(hash_key=dataset_title):
        return error_response(HTTPStatus.CONFLICT, f"dataset '{dataset_title}' already exists")

    # get dataset to update
    dataset_id = body[DATASET_ID_SHORT_KEY]
    try:
        dataset = datasets_model_class.get(
            hash_key=f"{DATASET_ID_PREFIX}{dataset_id}", consistent_read=True
        )
    except DoesNotExist:
        return error_response(HTTPStatus.NOT_FOUND, f"dataset '{dataset_id}' does not exist")

    # update dataset
    update_dataset_attributes(dataset, body)
    dataset.save()
    dataset.refresh(consistent_read=True)

    # return response
    resp_body = dataset.as_dict()

    return success_response(HTTPStatus.OK, resp_body)


def update_dataset_attributes(dataset: DatasetsModelBase, req_body: JsonObject) -> None:
    for attr in DatasetsModelBase.get_attributes():
        if attr in req_body and attr != "id":
            setattr(dataset, attr, req_body[attr])
