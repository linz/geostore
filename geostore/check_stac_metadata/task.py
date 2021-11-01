from json import dumps

from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from jsonschema import ValidationError, validate
from linz_logger import get_log

from ..api_keys import EVENT_KEY
from ..error_response_keys import ERROR_KEY, ERROR_MESSAGE_KEY
from ..parameter_store import ParameterName, get_param
from ..s3 import get_s3_client_for_role
from ..s3_utils import get_bucket_and_key_from_url
from ..step_function import get_hash_key
from ..step_function_keys import DATASET_ID_KEY, METADATA_URL_KEY, S3_ROLE_ARN_KEY, VERSION_ID_KEY
from ..types import JsonObject
from ..validation_results_model import ValidationResultFactory
from .utils import STACDatasetValidator

LOGGER = get_log()


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
                    S3_ROLE_ARN_KEY: {"type": "string"},
                },
                "required": [DATASET_ID_KEY, METADATA_URL_KEY, S3_ROLE_ARN_KEY, VERSION_ID_KEY],
                "additionalProperties": True,
            },
        )
    except ValidationError as error:
        LOGGER.warning(dumps({ERROR_KEY: error}, default=str))
        return {ERROR_MESSAGE_KEY: error.message}

    try:
        s3_client = get_s3_client_for_role(event[S3_ROLE_ARN_KEY])
    except ClientError as error:
        LOGGER.warning(dumps({ERROR_KEY: error}, default=str))
        return {ERROR_MESSAGE_KEY: str(error)}

    def s3_url_reader(url: str) -> StreamingBody:
        bucket_name, key = get_bucket_and_key_from_url(url)
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        return response["Body"]

    hash_key = get_hash_key(event[DATASET_ID_KEY], event[VERSION_ID_KEY])

    validation_result_factory = ValidationResultFactory(
        hash_key, get_param(ParameterName.STORAGE_VALIDATION_RESULTS_TABLE_NAME)
    )
    validator = STACDatasetValidator(hash_key, s3_url_reader, validation_result_factory)

    validator.run(event[METADATA_URL_KEY])
    return {}
