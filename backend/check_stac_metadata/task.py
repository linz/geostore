from json import dumps
from typing import TYPE_CHECKING

import boto3
from botocore.response import StreamingBody
from jsonschema import ValidationError, validate

from ..api_keys import EVENT_KEY
from ..error_response_keys import ERROR_KEY, ERROR_MESSAGE_KEY
from ..log import set_up_logging
from ..models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR, VERSION_ID_PREFIX
from ..parameter_store import ParameterName, get_param
from ..pystac_io_methods import get_bucket_and_key_from_url
from ..step_function import DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY
from ..types import JsonObject
from ..validation_results_model import ValidationResultFactory
from .utils import STACDatasetValidator

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_s3 import S3Client
else:
    # In production we want to avoid depending on a package which has no runtime impact
    S3Client = object

LOGGER = set_up_logging(__name__)
S3_CLIENT: S3Client = boto3.client("s3")


def s3_url_reader(url: str) -> StreamingBody:
    bucket_name, key = get_bucket_and_key_from_url(url)
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
                "additionalProperties": True,
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
