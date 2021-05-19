from json import dumps, loads
from typing import TYPE_CHECKING, Callable, Union
from urllib.parse import unquote_plus

from botocore.exceptions import ClientError  # type: ignore[import]

from .import_dataset_keys import NEW_KEY_KEY, ORIGINAL_KEY_KEY, TARGET_BUCKET_NAME_KEY
from .log import set_up_logging
from .types import JsonObject

if TYPE_CHECKING:
    from mypy_boto3_s3.type_defs import CopyObjectOutputTypeDef, PutObjectOutputTypeDef
else:
    CopyObjectOutputTypeDef = JsonObject
    PutObjectOutputTypeDef = JsonObject

INVOCATION_ID_KEY = "invocationId"
INVOCATION_SCHEMA_VERSION_KEY = "invocationSchemaVersion"
RESULTS_KEY = "results"
RESULT_CODE_KEY = "resultCode"
RESULT_STRING_KEY = "resultString"
S3_BUCKET_ARN_KEY = "s3BucketArn"
S3_KEY_KEY = "s3Key"
TASKS_KEY = "tasks"
TASK_ID_KEY = "taskId"

RESULT_CODE_PERMANENT_FAILURE = "PermanentFailure"

EXCEPTION_PREFIX = "Exception"

LOGGER = set_up_logging(__name__)


def get_import_result(
    event: JsonObject,
    importer: Callable[
        [str, str, str, str], Union[CopyObjectOutputTypeDef, PutObjectOutputTypeDef]
    ],
) -> JsonObject:
    LOGGER.debug(dumps(event))

    task = event[TASKS_KEY][0]
    source_bucket_name = task[S3_BUCKET_ARN_KEY].split(":::", maxsplit=1)[-1]
    parameters = loads(unquote_plus(task[S3_KEY_KEY]))

    try:
        response = importer(
            source_bucket_name,
            parameters[ORIGINAL_KEY_KEY],
            parameters[TARGET_BUCKET_NAME_KEY],
            parameters[NEW_KEY_KEY],
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

    return {
        INVOCATION_SCHEMA_VERSION_KEY: event[INVOCATION_SCHEMA_VERSION_KEY],
        "treatMissingKeysAs": RESULT_CODE_PERMANENT_FAILURE,
        INVOCATION_ID_KEY: event[INVOCATION_ID_KEY],
        RESULTS_KEY: [
            {
                TASK_ID_KEY: task[TASK_ID_KEY],
                RESULT_CODE_KEY: result_code,
                RESULT_STRING_KEY: result_string,
            }
        ],
    }
