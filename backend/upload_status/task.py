from json import dumps

from jsonschema import validate

from ..api_keys import SUCCESS_KEY
from ..import_file_batch_job_id_keys import ASSET_JOB_ID_KEY, METADATA_JOB_ID_KEY
from ..log import set_up_logging
from ..step_function import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_KEY,
    IMPORT_DATASET_KEY,
    JOB_STATUS_RUNNING,
    METADATA_UPLOAD_KEY,
    VALIDATION_KEY,
    VERSION_ID_KEY,
    get_tasks_status,
)
from ..types import JsonObject

INPUT_KEY = "input"
EXECUTION_ID_KEY = "execution_id"

LOGGER = set_up_logging(__name__)


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    LOGGER.debug(dumps({"event": event}))

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
        for key in [ASSET_UPLOAD_KEY, METADATA_UPLOAD_KEY]
        if key in raw_import_status
    }
