from json import dumps
from urllib.parse import urlparse

import boto3
from botocore.response import StreamingBody  # type: ignore[import]
from jsonschema import ValidationError, validate  # type: ignore[import]

from ..error_response_keys import ERROR_KEY, ERROR_MESSAGE_KEY
from ..log import set_up_logging
from ..models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR, VERSION_ID_PREFIX
from ..parameter_store import ParameterName, get_param
from ..step_function_event_keys import DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY
from ..types import JsonObject
from ..validation_results_model import ValidationResultFactory
from .utils import STACDatasetValidator

LOGGER = set_up_logging(__name__)
S3_CLIENT = boto3.client("s3")


def s3_url_reader(url: str) -> StreamingBody:
    parse_result = urlparse(url, allow_fragments=False)
    bucket_name = parse_result.netloc
    key = parse_result.path[1:]
    response = S3_CLIENT.get_object(Bucket=bucket_name, Key=key)
    return response["Body"]


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:

    LOGGER.debug(dumps({"event": event}))

    # validate input
    try:
        validate(
            event,
            {
                "type": "object",
                "properties": {
                    DATASET_ID_KEY: {"type": "string"},
                    VERSION_ID_KEY: {"type": "string"},
                    METADATA_URL_KEY: {"type": "string"},
                },
                "required": [DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY],
            },
        )
    except ValidationError as error:
        LOGGER.warning(dumps({ERROR_KEY: error}, default=str))
        return {ERROR_MESSAGE_KEY: error.message}

    hash_key = (
        f"{DATASET_ID_PREFIX}{event[DATASET_ID_KEY]}"
        f"{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{event[VERSION_ID_KEY]}"
    )

    validation_result_factory = ValidationResultFactory(
        hash_key, get_param(ParameterName.STORAGE_VALIDATION_RESULTS_TABLE_NAME)
    )
    validator = STACDatasetValidator(s3_url_reader, validation_result_factory)

    validator.run(event[METADATA_URL_KEY], hash_key)
    return {}
