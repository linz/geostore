from jsonschema import ValidationError, validate  # type: ignore[import]

from ..types import JsonObject


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    try:
        validate(
            event,
            {
                "type": "object",
                "properties": {"dataset_id": {"type": "string"}, "version_id": {"type": "string"}},
                "required": ["dataset_id", "version_id"],
            },
        )
    except ValidationError as error:
        return {"error message": error.message}

    resp = {"success": True, "message": ""}

    return resp
