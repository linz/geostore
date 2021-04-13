"""Delete dataset function."""
from http import HTTPStatus

from jsonschema import ValidationError, validate  # type: ignore[import]
from pynamodb.exceptions import DoesNotExist

from ..api_responses import error_response, success_response
from ..datasets_model import datasets_model_with_meta
from ..types import JsonObject


def delete_dataset(payload: JsonObject) -> JsonObject:
    """DELETE: Delete Dataset."""

    body_schema = {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}

    # request body validation
    req_body = payload["body"]
    try:
        validate(req_body, body_schema)
    except ValidationError as err:
        return error_response(HTTPStatus.BAD_REQUEST, err.message)

    datasets_model_class = datasets_model_with_meta()

    # get dataset to delete
    try:
        dataset = datasets_model_class.get(
            hash_key=f"DATASET#{req_body['id']}", consistent_read=True
        )
    except DoesNotExist:
        return error_response(HTTPStatus.NOT_FOUND, f"dataset '{req_body['id']}' does not exist")

    # delete dataset
    dataset.delete()

    return success_response(HTTPStatus.NO_CONTENT, {})
