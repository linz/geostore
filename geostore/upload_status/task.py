from json import dumps

from jsonschema import validate
from linz_logger import get_log

from ..api_keys import EVENT_KEY, SUCCESS_KEY
from ..import_file_batch_job_id_keys import ASSET_JOB_ID_KEY, METADATA_JOB_ID_KEY
from ..step_function import get_tasks_status
from ..step_function_keys import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_KEY,
    IMPORT_DATASET_KEY,
    JOB_STATUS_RUNNING,
    METADATA_UPLOAD_KEY,
    VALIDATION_KEY,
    VERSION_ID_KEY,
)
from ..types import JsonObject

INPUT_KEY = "input"
EXECUTION_ID_KEY = "execution_id"

LOGGER = get_log()


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    LOGGER.debug(dumps({EVENT_KEY: event}))

    validate(
        event,
        {
            "type": "object",
            "properties": {
                DATASET_ID_KEY: {"type": "string"},
                VERSION_ID_KEY: {"type": "string"},
                VALIDATION_KEY: {
                    "type": "object",
                    "properties": {SUCCESS_KEY: {"type": "boolean"}},
                    "required": [SUCCESS_KEY],
                },
                IMPORT_DATASET_KEY: {
                    "type": "object",
                    "properties": {
                        METADATA_JOB_ID_KEY: {"type": "string"},
                        ASSET_JOB_ID_KEY: {"type": "string"},
                    },
                    "required": [METADATA_JOB_ID_KEY, ASSET_JOB_ID_KEY],
                },
            },
            "required": [DATASET_ID_KEY, VERSION_ID_KEY, VALIDATION_KEY, IMPORT_DATASET_KEY],
        },
    )

    raw_import_status = get_tasks_status(
        JOB_STATUS_RUNNING,
        event[DATASET_ID_KEY],
        event[VERSION_ID_KEY],
        event[VALIDATION_KEY][SUCCESS_KEY],
        {
            METADATA_JOB_ID_KEY: event[IMPORT_DATASET_KEY][METADATA_JOB_ID_KEY],
            ASSET_JOB_ID_KEY: event[IMPORT_DATASET_KEY][ASSET_JOB_ID_KEY],
        },
    )
    return {
        key: raw_import_status[key]
        for key in [VALIDATION_KEY, ASSET_UPLOAD_KEY, METADATA_UPLOAD_KEY]
        if key in raw_import_status
    }
