from jsonschema import validate

from ..models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR, VERSION_ID_PREFIX
from ..parameter_store import ParameterName, get_param
from ..processing_assets_model import ProcessingAssetType, processing_assets_model_with_meta
from ..step_function_keys import DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY
from ..types import JsonObject

MAX_ITERATION_SIZE = 10_000

ASSETS_TABLE_NAME_KEY = "assets_table_name"
CONTENT_KEY = "content"
FIRST_ITEM_KEY = "first_item"
ITERATION_SIZE_KEY = "iteration_size"
NEXT_ITEM_KEY = "next_item"
RESULTS_TABLE_NAME_KEY = "results_table_name"

EVENT_SCHEMA = {
    "type": "object",
    "properties": {
        CONTENT_KEY: {
            "type": "object",
            "properties": {
                FIRST_ITEM_KEY: {"type": "string", "pattern": r"^\d+$"},
                ITERATION_SIZE_KEY: {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": MAX_ITERATION_SIZE,
                },
                NEXT_ITEM_KEY: {
                    "type": "integer",
                    "minimum": MAX_ITERATION_SIZE,
                    "multipleOf": MAX_ITERATION_SIZE,
                },
            },
            "required": [FIRST_ITEM_KEY, ITERATION_SIZE_KEY, NEXT_ITEM_KEY],
            "additionalProperties": False,
        },
        DATASET_ID_KEY: {"type": "string"},
        METADATA_URL_KEY: {"type": "string"},
        VERSION_ID_KEY: {"type": "string"},
    },
    "required": [DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY],
    "additionalProperties": True,
}


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    validate(event, EVENT_SCHEMA)

    if CONTENT_KEY in event.keys():
        assert int(event[CONTENT_KEY][FIRST_ITEM_KEY]) % MAX_ITERATION_SIZE == 0
        first_item_index = event[CONTENT_KEY][NEXT_ITEM_KEY]
    else:
        first_item_index = 0

    dataset_id = event[DATASET_ID_KEY]
    version_id = event[VERSION_ID_KEY]

    processing_assets_model = processing_assets_model_with_meta()

    asset_count = processing_assets_model.count(
        hash_key=(
            f"{DATASET_ID_PREFIX}{dataset_id}{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{version_id}"
        ),
        range_key_condition=processing_assets_model.sk.startswith(
            f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}"
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
        FIRST_ITEM_KEY: str(first_item_index),
        ITERATION_SIZE_KEY: iteration_size,
        NEXT_ITEM_KEY: next_item_index,
        ASSETS_TABLE_NAME_KEY: get_param(ParameterName.PROCESSING_ASSETS_TABLE_NAME),
        RESULTS_TABLE_NAME_KEY: get_param(ParameterName.STORAGE_VALIDATION_RESULTS_TABLE_NAME),
    }
