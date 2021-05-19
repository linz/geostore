from json import dumps, load, loads
from os.path import basename
from typing import Dict, Iterable
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
from ..stac_format import STAC_ASSETS_KEY, STAC_HREF_KEY, STAC_LINKS_KEY
from ..types import JsonObject

S3_CLIENT = boto3.client("s3")
LOGGER = set_up_logging(__name__)


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    LOGGER.debug(dumps(event))

    task = event[TASKS_KEY][0]
    source_bucket_name = task[S3_BUCKET_ARN_KEY].split(":::", maxsplit=1)[-1]
    parameters = loads(unquote_plus(task[S3_KEY_KEY]))

    try:
        get_object_response = S3_CLIENT.get_object(
            Bucket=source_bucket_name, Key=parameters[ORIGINAL_KEY_KEY]
        )
        assert "Body" in get_object_response, get_object_response

        metadata = load(get_object_response["Body"])

        assets = metadata.get(STAC_ASSETS_KEY, {}).values()
        change_href_to_basename(assets)

        links = metadata.get(STAC_LINKS_KEY, [])
        change_href_to_basename(links)

        put_object_response = S3_CLIENT.put_object(
            Bucket=parameters[TARGET_BUCKET_NAME_KEY],
            Key=parameters[NEW_KEY_KEY],
            Body=dumps(metadata).encode(),
        )
        result_code = "Succeeded"
        result_string = str(put_object_response)
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
                TASK_ID_KEY: (task[TASK_ID_KEY]),
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


def change_href_to_basename(items: Iterable[Dict[str, str]]) -> None:
    for item in items:
        item[STAC_HREF_KEY] = basename(item[STAC_HREF_KEY])
