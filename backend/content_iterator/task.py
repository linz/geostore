from jsonschema import validate  # type: ignore[import]

from ..processing_assets_model import ProcessingAssetType, ProcessingAssetsModel
from ..types import JsonObject

# From https://docs.aws.amazon.com/batch/latest/userguide/array_jobs.html
# TODO: Set MAX_ITERATION_SIZE to 10_000 once we figure out [how to set a numeric
# size](https://stackoverflow.com/q/66202138/96588)
MAX_ITERATION_SIZE = 5

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
                    "minimum": 1,
                    "multipleOf": MAX_ITERATION_SIZE,
                },
            },
            "required": ["first_item", "iteration_size", "next_item"],
            "additionalProperties": False,
        },
        "dataset_id": {"type": "string"},
        "metadata_url": {"type": "string"},
        "type": {"type": "string"},
        "version_id": {"type": "string"},
    },
    "required": ["dataset_id", "metadata_url", "type", "version_id"],
    "additionalProperties": False,
}


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    validate(event, EVENT_SCHEMA)

    if "content" in event.keys():
        assert int(event["content"]["first_item"]) % MAX_ITERATION_SIZE == 0
        first_item_index = event["content"]["next_item"]
    else:
        first_item_index = 0

    dataset_id = event["dataset_id"]
    version_id = event["version_id"]

    asset_count = ProcessingAssetsModel.count(
        hash_key=f"DATASET#{dataset_id}#VERSION#{version_id}",
        range_key_condition=ProcessingAssetsModel.sk.startswith(
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
