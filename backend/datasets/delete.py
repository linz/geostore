"""Delete dataset function."""

from jsonschema import ValidationError, validate  # type: ignore[import]
from pynamodb.exceptions import DoesNotExist

from ..api_responses import error_response, success_response
from ..dataset import DATASET_TYPES
from ..datasets_model import DatasetsModel
from ..types import JsonObject


def delete_dataset(payload: JsonObject) -> JsonObject:
    """DELETE: Delete Dataset."""

    body_schema = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "type": {
                "type": "string",
                "enum": DATASET_TYPES,
            },
        },
        "required": ["id", "type"],
    }

    # request body validation
    req_body = payload["body"]
    try:
        validate(req_body, body_schema)
    except ValidationError as err:
        return error_response(400, err.message)

    # get dataset to delete
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

    # delete dataset
    dataset.delete()

    return success_response(204, {})
