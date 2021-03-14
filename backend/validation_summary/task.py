from jsonschema import ValidationError, validate  # type: ignore[import]

from ..types import JsonObject
from ..validation_results_model import ValidationResult, ValidationResultsModel


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

    if ValidationResultsModel.validation_outcome_index.count(
        f"DATASET#{event['dataset_id']}#VERSION#{event['version_id']}",
        range_key_condition=ValidationResultsModel.result == ValidationResult.FAILED.value,
        limit=1,
    ):
        return {"success": False}

    return {"success": True}
