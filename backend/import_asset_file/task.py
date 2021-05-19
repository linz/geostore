from json import dumps, loads
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError  # type: ignore[import]

from ..import_dataset_keys import (
    EXCEPTION_PREFIX,
    INVOCATION_ID_KEY,
    INVOCATION_SCHEMA_VERSION_KEY,
    NEW_KEY_KEY,
    ORIGINAL_KEY_KEY,
    RESULTS_KEY,
    RESULT_CODE_KEY,
    RESULT_CODE_PERMANENT_FAILURE,
    RESULT_STRING_KEY,
    S3_BUCKET_ARN_KEY,
    S3_KEY_KEY,
    TARGET_BUCKET_NAME_KEY,
    TASKS_KEY,
    TASK_ID_KEY,
    TREAT_MISSING_KEYS_AS_KEY,
)
from ..log import set_up_logging
from ..types import JsonObject

S3_CLIENT = boto3.client("s3")
LOGGER = set_up_logging(__name__)


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    LOGGER.debug(dumps(event))

    task = event[TASKS_KEY][0]
    source_bucket_name = task[S3_BUCKET_ARN_KEY].split(":::", maxsplit=1)[-1]
    parameters = loads(unquote_plus(task[S3_KEY_KEY]))

    try:
        response = S3_CLIENT.copy_object(
            CopySource={"Bucket": source_bucket_name, "Key": parameters[ORIGINAL_KEY_KEY]},
            Bucket=parameters[TARGET_BUCKET_NAME_KEY],
            Key=parameters[NEW_KEY_KEY],
        )
        result_code = "Succeeded"
        result_string = str(response)
    except ClientError as error:
        error_code = error.response["Error"]["Code"]
        if error_code == "RequestTimeout":
            result_code = "TemporaryFailure"
            result_string = "Retry request to Amazon S3 due to timeout."
        else:
            result_code = RESULT_CODE_PERMANENT_FAILURE
            result_string = f"{error_code}: {error.response['Error']['Message']}"
    except Exception as error:  # pylint:disable=broad-except
        result_code = RESULT_CODE_PERMANENT_FAILURE
        result_string = f"{EXCEPTION_PREFIX}: {error}"
    finally:
        results = [
            {
                TASK_ID_KEY: task[TASK_ID_KEY],
                RESULT_CODE_KEY: result_code,
                RESULT_STRING_KEY: result_string,
            }
        ]

    return {
        INVOCATION_SCHEMA_VERSION_KEY: event[INVOCATION_SCHEMA_VERSION_KEY],
        TREAT_MISSING_KEYS_AS_KEY: RESULT_CODE_PERMANENT_FAILURE,
        INVOCATION_ID_KEY: event[INVOCATION_ID_KEY],
        RESULTS_KEY: results,
    }
