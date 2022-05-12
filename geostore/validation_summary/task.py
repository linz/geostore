from logging import Logger

from jsonschema import ValidationError, validate
from linz_logger import get_log

from ..api_keys import SUCCESS_KEY
from ..error_response_keys import ERROR_MESSAGE_KEY
from ..logging_keys import LOG_MESSAGE_LAMBDA_START, LOG_MESSAGE_VALIDATION_COMPLETE
from ..models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR, VERSION_ID_PREFIX
from ..step_function import Outcome
from ..step_function_keys import DATASET_ID_KEY, VERSION_ID_KEY
from ..types import JsonObject
from ..validation_results_model import ValidationResult, validation_results_model_with_meta

LOGGER: Logger = get_log()


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    LOGGER.debug(LOG_MESSAGE_LAMBDA_START, extra={"lambda_input": event})

    try:
        validate(
            event,
            {
                "type": "object",
                "properties": {
                    DATASET_ID_KEY: {"type": "string"},
                    VERSION_ID_KEY: {"type": "string"},
                },
                "required": [DATASET_ID_KEY, VERSION_ID_KEY],
            },
        )
    except ValidationError as error:
        return {ERROR_MESSAGE_KEY: error.message}

    validation_results_model = validation_results_model_with_meta()
    success = not bool(
        validation_results_model.validation_outcome_index.count(
            (
                f"{DATASET_ID_PREFIX}{event['dataset_id']}"
                f"{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{event['version_id']}"
            ),
            range_key_condition=validation_results_model.result == ValidationResult.FAILED.value,
            limit=1,
        )
    )

    result = {SUCCESS_KEY: success}
    LOGGER.debug(LOG_MESSAGE_VALIDATION_COMPLETE, extra={"outcome": Outcome.PASSED})
    return result
