from jsonschema import validate  # type: ignore[import]

from ..processing_assets_model import ProcessingAssetType, processing_assets_model_with_meta
from ..step_function_event_keys import DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY
from ..types import JsonObject

MAX_ITERATION_SIZE = 10_000

EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        "content": {
            "type": "object",
            "properties": {
                "first_item": {"type": "string", "pattern": r"^\d+$"},
                "iteration_size": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_ITERATION_SIZE,
                },
                "next_item": {
                    "type": "integer",
                    "minimum": MAX_ITERATION_SIZE,
                    "multipleOf": MAX_ITERATION_SIZE,
                },
            },
            "required": ["first_item", "iteration_size", "next_item"],
            "additionalProperties": False,
        },
        DATASET_ID_KEY: {"type": "string"},
        METADATA_URL_KEY: {"type": "string"},
        VERSION_ID_KEY: {"type": "string"},
    },
    "required": [DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY],
    "additionalProperties": False,
}


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    validate(event, EVENT_SCHEMA)

    if "content" in event.keys():
        assert int(event["content"]["first_item"]) % MAX_ITERATION_SIZE == 0
        first_item_index = event["content"]["next_item"]
    else:
        first_item_index = 0

    dataset_id = event[DATASET_ID_KEY]
    version_id = event[VERSION_ID_KEY]

    processing_assets_model = processing_assets_model_with_meta()

    asset_count = processing_assets_model.count(
        hash_key=f"DATASET#{dataset_id}#VERSION#{version_id}",
        range_key_condition=processing_assets_model.sk.startswith(
            f"{ProcessingAssetType.DATA.value}#"
        ),
    )

    remaining_assets = asset_count - first_item_index
    if remaining_assets > MAX_ITERATION_SIZE:
        next_item_index = first_item_index + MAX_ITERATION_SIZE
        iteration_size = MAX_ITERATION_SIZE
    else:
        next_item_index = -1
        iteration_size = remaining_assets

    return {
        "first_item": str(first_item_index),
        "iteration_size": iteration_size,
        "next_item": next_item_index,
    }
