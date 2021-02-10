from copy import copy
from typing import Any, MutableMapping

from jsonschema import validate  # type: ignore[import]

from ..assets_model import ProcessingAssetsModel

ITERATION_SIZE = 2

JSON_OBJECT = MutableMapping[str, Any]
EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        "content": {
            "type": "object",
            "properties": {
                "first_item": {"type": "integer"},
                "iteration_size": {"type": "integer"},
                "next_item": {"$ref": "#/definitions/nonNegativeInteger"},
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
    "definitions": {"nonNegativeInteger": {"type": "integer", "minimum": 0}},
}


def lambda_handler(event: JSON_OBJECT, _context: bytes) -> JSON_OBJECT:
    validate(event, EVENT_SCHEMA)

    if "content" in event.keys():
        first_item = int(event["content"]["next_item"])
    else:
        first_item = 0

    dataset_id = event["dataset_id"]
    version_id = event["version_id"]

    total_size = ProcessingAssetsModel.count(hash_key=f"DATASET#{dataset_id}#VERSION#{version_id}")

    if first_item + ITERATION_SIZE < total_size:
        next_item = first_item + ITERATION_SIZE
    else:
        next_item = -1

    result = copy(event)
    result["content"] = {
        "first_item": first_item,
        "next_item": next_item,
        "iteration_size": ITERATION_SIZE,
    }
    return result
