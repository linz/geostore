"""Create dataset function."""
from http import HTTPStatus

from jsonschema import ValidationError, validate
from pystac.stac_io import StacIO

from ..api_responses import error_response, success_response
from ..dataset_properties import TITLE_PATTERN
from ..datasets_model import datasets_model_with_meta
from ..pystac_io_methods import S3StacIO
from ..step_function_keys import DESCRIPTION_KEY, TITLE_KEY
from ..types import JsonObject

StacIO.set_default(S3StacIO)


def create_dataset(body: JsonObject) -> JsonObject:
    """POST: Create Dataset."""

    body_schema = {
        "type": "object",
        "properties": {
            TITLE_KEY: {"type": "string", "pattern": TITLE_PATTERN},
            DESCRIPTION_KEY: {"type": "string"},
        },
        "required": [TITLE_KEY, DESCRIPTION_KEY],
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

    # create dataset
    dataset = datasets_model_class(title=dataset_title)
    dataset.save()
    dataset.refresh(consistent_read=True)

    # return response
    resp_body = dataset.as_dict()

    return success_response(HTTPStatus.CREATED, resp_body)
