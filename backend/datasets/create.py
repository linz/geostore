"""Create dataset function."""
from http import HTTPStatus
from string import ascii_letters, digits

from jsonschema import ValidationError, validate  # type: ignore[import]

from ..api_responses import error_response, success_response
from ..datasets_model import datasets_model_with_meta
from ..types import JsonObject

TITLE_CHARACTERS = f"{ascii_letters}{digits}_-"
TITLE_PATTERN = f"^[{TITLE_CHARACTERS}]+$"


def create_dataset(payload: JsonObject) -> JsonObject:
    """POST: Create Dataset."""

    body_schema = {
        "type": "object",
        "properties": {"title": {"type": "string", "pattern": TITLE_PATTERN}},
        "required": ["title"],
    }

    # request body validation
    req_body = payload["body"]
    try:
        validate(req_body, body_schema)
    except ValidationError as err:
        return error_response(HTTPStatus.BAD_REQUEST, err.message)

    # check for duplicate type/title
    datasets_model_class = datasets_model_with_meta()
    if datasets_model_class.datasets_title_idx.count(hash_key=req_body["title"]):
        return error_response(HTTPStatus.CONFLICT, f"dataset '{req_body['title']}' already exists")

    # create dataset
    dataset = datasets_model_class(title=req_body["title"])
    dataset.save()
    dataset.refresh(consistent_read=True)

    # return response
    resp_body = dataset.as_dict()

    return success_response(HTTPStatus.CREATED, resp_body)
