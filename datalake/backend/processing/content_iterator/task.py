from typing import Any, MutableMapping

from jsonschema import validate  # type: ignore[import]

from ..assets_model import ProcessingAssetsModel

JSON_OBJECT = MutableMapping[str, Any]
EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        "content": {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "version_id": {"type": "string"},
                "next_item": {"$ref": "#/definitions/nonNegativeInteger"},
            },
            "required": ["dataset_id", "version_id", "next_item"],
        }
    },
    "required": ["content"],
    "additionalProperties": False,
    "definitions": {"nonNegativeInteger": {"type": "integer", "minimum": 0}},
}


def lambda_handler(event: JSON_OBJECT, _context: bytes) -> JSON_OBJECT:
    validate(event, EVENT_SCHEMA)

    iteration_size = 2

    if "content" in event.keys():
        first_item = int(event["content"]["next_item"])
    else:
        first_item = 0

    event_content = event["content"]
    dataset_id = event_content["dataset_id"]
    version_id = event_content["version_id"]

    total_size = ProcessingAssetsModel.count(hash_key=f"DATASET#{dataset_id}#VERSION#{version_id}")

    if (first_item + iteration_size) <= total_size:
        next_item = first_item + iteration_size
    else:
        next_item = -1

    return {"first_item": first_item, "next_item": next_item, "iteration_size": iteration_size}
