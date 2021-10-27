from json import dumps, loads
from typing import TYPE_CHECKING, Callable, Optional
from urllib.parse import unquote_plus

from botocore.exceptions import ClientError
from linz_logger import get_log

from .aws_response import AWS_CODE_REQUEST_TIMEOUT
from .import_dataset_keys import NEW_KEY_KEY, ORIGINAL_KEY_KEY, TARGET_BUCKET_NAME_KEY
from .s3 import get_s3_client_for_role
from .step_function_keys import S3_ROLE_ARN_KEY
from .types import JsonObject

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import PutObjectOutputTypeDef
else:
    PutObjectOutputTypeDef = JsonObject  # pragma: no mutate
    S3Client = object  # pragma: no mutate

INVOCATION_ID_KEY = "invocationId"
INVOCATION_SCHEMA_VERSION_KEY = "invocationSchemaVersion"
RESULTS_KEY = "results"
RESULT_CODE_KEY = "resultCode"
RESULT_STRING_KEY = "resultString"
S3_BUCKET_ARN_KEY = "s3BucketArn"
S3_KEY_KEY = "s3Key"
TASKS_KEY = "tasks"
TASK_ID_KEY = "taskId"
TREAT_MISSING_KEYS_AS_KEY = "treatMissingKeysAs"

RESULT_CODE_PERMANENT_FAILURE = "PermanentFailure"
RESULT_CODE_SUCCEEDED = "Succeeded"
RESULT_CODE_TEMPORARY_FAILURE = "TemporaryFailure"

EXCEPTION_PREFIX = "Exception"
RETRY_RESULT_STRING = "Retry request to Amazon S3 due to timeout."

LOGGER = get_log()


def get_import_result(
    event: JsonObject,
    importer: Callable[[str, str, str, str, S3Client], Optional[PutObjectOutputTypeDef]],
) -> JsonObject:
    LOGGER.debug(dumps(event))

    task = event[TASKS_KEY][0]
    source_bucket_name = task[S3_BUCKET_ARN_KEY].split(":::", maxsplit=1)[-1]
    parameters = loads(unquote_plus(task[S3_KEY_KEY]))
    source_s3_client = get_s3_client_for_role(parameters[S3_ROLE_ARN_KEY])

    try:
        response = importer(
            source_bucket_name,
            parameters[ORIGINAL_KEY_KEY],
            parameters[TARGET_BUCKET_NAME_KEY],
            parameters[NEW_KEY_KEY],
            source_s3_client,
        )
        result_code = RESULT_CODE_SUCCEEDED
        result_string = str(response)
    except ClientError as error:
        error_code = error.response["Error"]["Code"]
        if error_code == AWS_CODE_REQUEST_TIMEOUT:
            result_code = RESULT_CODE_TEMPORARY_FAILURE
            result_string = RETRY_RESULT_STRING
        else:
            result_code = RESULT_CODE_PERMANENT_FAILURE
            error_message = error.response["Error"]["Message"]
            result_string = f"{error_code} when calling {error.operation_name}: {error_message}"
    except Exception as error:  # pylint:disable=broad-except
        result_code = RESULT_CODE_PERMANENT_FAILURE
        result_string = f"{EXCEPTION_PREFIX}: {error}"

    result = {
        INVOCATION_SCHEMA_VERSION_KEY: event[INVOCATION_SCHEMA_VERSION_KEY],
        TREAT_MISSING_KEYS_AS_KEY: RESULT_CODE_PERMANENT_FAILURE,
        INVOCATION_ID_KEY: event[INVOCATION_ID_KEY],
        RESULTS_KEY: [
            {
                TASK_ID_KEY: task[TASK_ID_KEY],
                RESULT_CODE_KEY: result_code,
                RESULT_STRING_KEY: result_string,
            }
        ],
    }
    LOGGER.debug(dumps(result))
    return result
