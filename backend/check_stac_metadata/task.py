from json import dumps
from urllib.parse import urlparse

import boto3
from botocore.response import StreamingBody  # type: ignore[import]
from jsonschema import ValidationError, validate  # type: ignore[import]

from backend.keys.step_function_event_keys import DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY

from ..import_dataset.task import EVENT_KEY
from ..keys.error_response_keys import ERROR_KEY, ERROR_MESSAGE_KEY
from ..log import set_up_logging
from ..types import JsonObject
from ..validation_results_model import ValidationResultFactory
from .utils import STACDatasetValidator, parse_arguments

LOGGER = set_up_logging(__name__)
S3_CLIENT = boto3.client("s3")


def s3_url_reader(url: str) -> StreamingBody:
    parse_result = urlparse(url, allow_fragments=False)
    bucket_name = parse_result.netloc
    key = parse_result.path[1:]
    response = S3_CLIENT.get_object(Bucket=bucket_name, Key=key)
    return response["Body"]


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:

    LOGGER.debug(dumps({EVENT_KEY: event}))

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

    arguments = parse_arguments(LOGGER)

    hash_key = f"DATASET#{arguments.dataset_id}#VERSION#{arguments.version_id}"
    validation_result_factory = ValidationResultFactory(hash_key)
    validator = STACDatasetValidator(s3_url_reader, validation_result_factory)

    validator.run(arguments.metadata_url, hash_key)
    return {}
