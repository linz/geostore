from logging import Logger

from botocore.exceptions import ClientError
from jsonschema import ValidationError, validate
from linz_logger import get_log

from ..api_keys import SUCCESS_KEY
from ..error_response_keys import ERROR_MESSAGE_KEY
from ..logging_keys import (
    GIT_COMMIT,
    LOG_MESSAGE_LAMBDA_FAILURE,
    LOG_MESSAGE_LAMBDA_START,
    LOG_MESSAGE_VALIDATION_COMPLETE,
)
from ..parameter_store import ParameterName, get_param
from ..processing_assets_model import ProcessingAssetType
from ..s3_utils import get_s3_url_reader
from ..step_function import AssetGarbageCollector, Outcome, get_hash_key
from ..step_function_keys import (
    CURRENT_VERSION_ID_KEY,
    DATASET_ID_KEY,
    DATASET_TITLE_KEY,
    METADATA_URL_KEY,
    NEW_VERSION_ID_KEY,
    S3_ROLE_ARN_KEY,
)
from ..types import JsonObject
from ..validation_results_model import ValidationResultFactory
from .utils import STACDatasetValidator

LOGGER: Logger = get_log()


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    LOGGER.debug(
        LOG_MESSAGE_LAMBDA_START,
        extra={"lambda_input": event, GIT_COMMIT: get_param(ParameterName.GIT_COMMIT)},
    )

    # validate input
    try:
        validate(
            event,
            {
                "type": "object",
                "properties": {
                    CURRENT_VERSION_ID_KEY: {"type": "string"},
                    DATASET_ID_KEY: {"type": "string"},
                    DATASET_TITLE_KEY: {"type": "string"},
                    METADATA_URL_KEY: {"type": "string"},
                    NEW_VERSION_ID_KEY: {"type": "string"},
                    S3_ROLE_ARN_KEY: {"type": "string"},
                },
                "required": [
                    CURRENT_VERSION_ID_KEY,
                    DATASET_ID_KEY,
                    DATASET_TITLE_KEY,
                    METADATA_URL_KEY,
                    NEW_VERSION_ID_KEY,
                    S3_ROLE_ARN_KEY,
                ],
                "additionalProperties": True,
            },
        )
    except ValidationError as error:
        LOGGER.warning(
            LOG_MESSAGE_VALIDATION_COMPLETE,
            extra={
                "outcome": Outcome.FAILED,
                "error": error,
                GIT_COMMIT: get_param(ParameterName.GIT_COMMIT),
            },
        )
        return {ERROR_MESSAGE_KEY: error.message}

    try:
        s3_url_reader = get_s3_url_reader(event[S3_ROLE_ARN_KEY], event[DATASET_TITLE_KEY], LOGGER)
    except ClientError as error:
        LOGGER.warning(
            LOG_MESSAGE_LAMBDA_FAILURE,
            extra={"error": error, GIT_COMMIT: get_param(ParameterName.GIT_COMMIT)},
        )
        return {ERROR_MESSAGE_KEY: str(error)}

    asset_garbage_collector = AssetGarbageCollector(
        event[DATASET_ID_KEY],
        event[CURRENT_VERSION_ID_KEY],
        ProcessingAssetType.METADATA,
        LOGGER,
    )

    hash_key = get_hash_key(event[DATASET_ID_KEY], event[NEW_VERSION_ID_KEY])

    validation_result_factory = ValidationResultFactory(
        hash_key, get_param(ParameterName.STORAGE_VALIDATION_RESULTS_TABLE_NAME)
    )

    validator = STACDatasetValidator(
        hash_key, s3_url_reader, asset_garbage_collector, validation_result_factory
    )

    validator.run(event[METADATA_URL_KEY])
    return {SUCCESS_KEY: True}
