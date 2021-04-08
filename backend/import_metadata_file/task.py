from json import dumps, load, loads
from os.path import basename
from typing import Dict, Iterable
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import ClientError  # type: ignore[import]

from backend.import_dataset_keys import NEW_KEY_KEY, ORIGINAL_KEY_KEY

from ..log import set_up_logging
from ..parameter_store import ParameterName, get_param
from ..types import JsonObject

S3_CLIENT = boto3.client("s3")
TARGET_BUCKET = get_param(ParameterName.STORAGE_BUCKET_NAME)
LOGGER = set_up_logging(__name__)


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    LOGGER.debug(dumps(event))

    task = event["tasks"][0]
    source_bucket_name = task["s3BucketArn"].split(":::", maxsplit=1)[-1]
    parameters = loads(unquote_plus(task["s3Key"]))

    try:
        get_object_response = S3_CLIENT.get_object(
            Bucket=source_bucket_name, Key=parameters[ORIGINAL_KEY_KEY]
        )
        assert "Body" in get_object_response, get_object_response

        metadata = load(get_object_response["Body"])

        assets = metadata.get("assets", {}).values()
        change_href_to_basename(assets)

        links = metadata.get("links", [])
        change_href_to_basename(links)

        put_object_response = S3_CLIENT.put_object(
            Bucket=TARGET_BUCKET, Key=parameters[NEW_KEY_KEY], Body=dumps(metadata).encode()
        )
        result_code = "Succeeded"
        result_string = str(put_object_response)
    except ClientError as error:
        error_code = error.response["Error"]["Code"]
        if error_code == "RequestTimeout":
            result_code = "TemporaryFailure"
            result_string = "Retry request to Amazon S3 due to timeout."
        else:
            result_code = "PermanentFailure"
            result_string = f"{error_code}: {error.response['Error']['Message']}"
    except Exception as error:  # pylint:disable=broad-except
        result_code = "PermanentFailure"
        result_string = "Exception: {}".format(error)
    finally:
        results = [
            {"taskId": (task["taskId"]), "resultCode": result_code, "resultString": result_string}
        ]

    return {
        "invocationSchemaVersion": event["invocationSchemaVersion"],
        "treatMissingKeysAs": "PermanentFailure",
        "invocationId": event["invocationId"],
        "results": results,
    }


def change_href_to_basename(items: Iterable[Dict[str, str]]) -> None:
    for item in items:
        item["href"] = basename(item["href"])
