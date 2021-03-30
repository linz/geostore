from json import dumps
from os.path import basename
from urllib.parse import quote, urlparse
from uuid import uuid4

import boto3
from jsonschema import ValidationError, validate  # type: ignore[import]
from smart_open import open as smart_open  # type: ignore[import]

from ..import_dataset_keys import NEW_KEY_KEY, ORIGINAL_KEY_KEY
from ..log import set_up_logging
from ..parameter_store import ParameterName, get_param
from ..processing_assets_model import ProcessingAssetsModel
from ..types import JsonObject

LOGGER = set_up_logging(__name__)

STS_CLIENT = boto3.client("sts")
S3_CLIENT = boto3.client("s3")
S3CONTROL_CLIENT = boto3.client("s3control")

DATASET_ID_KEY = "dataset_id"
VERSION_ID_KEY = "version_id"
METADATA_URL_KEY = "metadata_url"

EVENT_KEY = "event"
ERROR_KEY = "error"

ERROR_MESSAGE_KEY = "error message"

JOB_ID_KEY = "job_id"


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    """Main Lambda entry point."""
    # pylint: disable=too-many-locals
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

    dataset_id = event[DATASET_ID_KEY]
    dataset_version_id = event[VERSION_ID_KEY]
    metadata_url = event[METADATA_URL_KEY]

    storage_bucket_name = get_param(ParameterName.STORAGE_BUCKET_NAME)
    storage_bucket_arn = f"arn:aws:s3:::{storage_bucket_name}"

    source_bucket_name = urlparse(metadata_url).netloc
    manifest_key = f"manifests/{dataset_version_id}.csv"

    with smart_open(f"s3://{storage_bucket_name}/{manifest_key}", "w") as s3_manifest:
        for item in ProcessingAssetsModel.query(
            f"DATASET#{dataset_id}#VERSION#{dataset_version_id}"
        ):
            LOGGER.debug(dumps({"Adding file to manifest": item.url}))
            key = s3_url_to_key(item.url)
            task_parameters = {
                ORIGINAL_KEY_KEY: key,
                NEW_KEY_KEY: f"{dataset_id}/{dataset_version_id}/{basename(key)}",
            }
            row = ",".join([source_bucket_name, quote(dumps(task_parameters))])
            s3_manifest.write(f"{row}\n")

    caller_identity = STS_CLIENT.get_caller_identity()
    assert "Account" in caller_identity, caller_identity
    account_number = caller_identity["Account"]

    manifest_s3_object = S3_CLIENT.head_object(Bucket=storage_bucket_name, Key=manifest_key)
    assert "ETag" in manifest_s3_object, manifest_s3_object
    manifest_s3_etag = manifest_s3_object["ETag"]

    s3_batch_copy_role_arn = get_param(ParameterName.IMPORT_DATASET_ROLE_ARN)
    import_dataset_file_task_arn = get_param(ParameterName.IMPORT_DATASET_FILE_FUNCTION_TASK_ARN)

    # trigger s3 batch copy operation
    response = S3CONTROL_CLIENT.create_job(
        AccountId=account_number,
        ConfirmationRequired=False,
        Operation={"LambdaInvoke": {"FunctionArn": import_dataset_file_task_arn}},
        Manifest={
            "Spec": {
                "Format": "S3BatchOperations_CSV_20180820",
                "Fields": ["Bucket", "Key"],
            },
            "Location": {
                "ObjectArn": f"{storage_bucket_arn}/{manifest_key}",
                "ETag": manifest_s3_etag,
            },
        },
        Report={
            "Enabled": True,
            "Bucket": storage_bucket_arn,
            "Format": "Report_CSV_20180820",
            "Prefix": f"reports/{dataset_version_id}",
            "ReportScope": "AllTasks",
        },
        Priority=1,
        RoleArn=s3_batch_copy_role_arn,
        ClientRequestToken=uuid4().hex,
    )
    LOGGER.debug(dumps({"s3 batch response": response}, default=str))

    return {JOB_ID_KEY: response["JobId"]}


def s3_url_to_key(url: str) -> str:
    return urlparse(url).path[1:]
