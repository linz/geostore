from typing import Any, MutableMapping

from jsonschema import validate  # type: ignore[import]

from ..assets_model import ProcessingAssetsModel

MAX_ITERATION_SIZE = 5

JSON_OBJECT = MutableMapping[str, Any]
EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        "content": {
            "type": "object",
            "properties": {
                "first_item": {"type": "string", "pattern": r"^\d+$"},
                "iteration_size": {"type": "string", "pattern": r"^\d+$"},
                "next_item": {"type": "string", "pattern": r"^\d+$"},
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


def lambda_handler(event: JSON_OBJECT, _context: bytes) -> JSON_OBJECT:
    validate(event, EVENT_SCHEMA)

    if "content" in event.keys():
        assert int(event["content"]["first_item"]) % MAX_ITERATION_SIZE == 0
        assert 0 < int(event["content"]["iteration_size"]) <= MAX_ITERATION_SIZE
        first_item_index = int(event["content"]["next_item"])
        assert first_item_index != 0
    else:
        first_item_index = 0

    dataset_id = event["dataset_id"]
    version_id = event["version_id"]

    asset_count = ProcessingAssetsModel.count(hash_key=f"DATASET#{dataset_id}#VERSION#{version_id}")

    remaining_assets = asset_count - first_item_index
    if remaining_assets > MAX_ITERATION_SIZE:
        next_item_index = first_item_index + MAX_ITERATION_SIZE
        iteration_size = MAX_ITERATION_SIZE
    else:
        next_item_index = -1
        iteration_size = max(remaining_assets, 2)

    return {
        "first_item": str(first_item_index),
        "iteration_size": str(iteration_size),
        "next_item": str(next_item_index),
    }
