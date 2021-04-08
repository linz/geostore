from json import dumps, loads
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
        response = S3_CLIENT.copy_object(
            CopySource={"Bucket": source_bucket_name, "Key": parameters[ORIGINAL_KEY_KEY]},
            Bucket=TARGET_BUCKET,
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
