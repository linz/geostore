from typing import Any, MutableMapping, Union

from jsonschema import validate  # type: ignore[import]

JSON_OBJECT = MutableMapping[str, Any]
EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        "content": {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "string"},
                "dataset_version_id": {"type": "string"},
            },
            "required": ["dataset_id", "dataset_version_id"],
        }
    },
    "required": ["content"],
    "additionalProperties": False,
}


def lambda_handler(event: JSON_OBJECT, _context: bytes) -> JSON_OBJECT:
    validate(event, EVENT_SCHEMA)

    total_size = 6
    iteration_size = 2

    if "content" in event.keys():
        first_item = int(event["content"]["next_item"])
    else:
        first_item = 0

    if (first_item + iteration_size) <= total_size:
        next_item = first_item + iteration_size
    else:
        next_item = -1

    resp: MutableMapping[str, Union[int, str]] = {}

    # "first_item" value must be string. It is directly passed as value to Batch job environment
    # variable BATCH_JOB_FIRST_ITEM_INDEX. All environment variables must be string and there is no
    # chance of conversion.
    resp["first_item"] = str(first_item)

    resp["next_item"] = next_item
    resp["iteration_size"] = iteration_size

    return resp
