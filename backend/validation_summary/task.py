from json import dumps

from jsonschema import ValidationError, validate  # type: ignore[import]

from ..api_keys import SUCCESS_KEY
from ..error_response_keys import ERROR_MESSAGE_KEY
from ..log import set_up_logging
from ..step_function_event_keys import DATASET_ID_KEY, VERSION_ID_KEY
from ..types import JsonObject
from ..validation_results_model import ValidationResult, validation_results_model_with_meta

LOGGER = set_up_logging(__name__)


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    LOGGER.debug(dumps({"event": event}))

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
        validation_results_model.validation_outcome_index.count(  # pylint: disable=no-member
            f"DATASET#{event['dataset_id']}#VERSION#{event['version_id']}",
            range_key_condition=validation_results_model.result == ValidationResult.FAILED.value,
            limit=1,
        )
    )

    result = {SUCCESS_KEY: success}
    LOGGER.debug(dumps(result))
    return result
