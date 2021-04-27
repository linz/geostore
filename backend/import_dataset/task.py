from functools import lru_cache
from json import dumps
from os.path import basename
from typing import TYPE_CHECKING
from urllib.parse import quote, urlparse
from uuid import uuid4

import boto3
from jsonschema import ValidationError, validate  # type: ignore[import]
from smart_open import open as smart_open  # type: ignore[import]

from ..error_response_keys import ERROR_KEY, ERROR_MESSAGE_KEY
from ..import_dataset_keys import NEW_KEY_KEY, ORIGINAL_KEY_KEY, TARGET_BUCKET_NAME_KEY
from ..import_file_batch_job_id_keys import ASSET_JOB_ID_KEY, METADATA_JOB_ID_KEY
from ..log import set_up_logging
from ..parameter_store import ParameterName, get_param
from ..processing_assets_model import ProcessingAssetType, processing_assets_model_with_meta
from ..step_function_event_keys import DATASET_ID_KEY, METADATA_URL_KEY, VERSION_ID_KEY
from ..types import JsonObject

if TYPE_CHECKING:
    from mypy_boto3_s3control.type_defs import (
        JobManifestLocationTypeDef,
        JobManifestSpecTypeDef,
        JobManifestTypeDef,
        JobOperationTypeDef,
        JobReportTypeDef,
        LambdaInvokeOperationTypeDef,
    )
else:
    JobManifestLocationTypeDef = dict
    JobManifestSpecTypeDef = dict
    JobManifestTypeDef = dict
    JobOperationTypeDef = dict
    JobReportTypeDef = dict
    LambdaInvokeOperationTypeDef = dict

LOGGER = set_up_logging(__name__)

STS_CLIENT = boto3.client("sts")
S3_CLIENT = boto3.client("s3")
S3CONTROL_CLIENT = boto3.client("s3control")

IMPORT_ASSET_FILE_TASK_ARN = get_param(ParameterName.PROCESSING_IMPORT_ASSET_FILE_FUNCTION_TASK_ARN)
IMPORT_METADATA_FILE_TASK_ARN = get_param(
    ParameterName.PROCESSING_IMPORT_METADATA_FILE_FUNCTION_TASK_ARN
)

STORAGE_BUCKET_NAME = get_param(ParameterName.STORAGE_BUCKET_NAME)
STORAGE_BUCKET_ARN = f"arn:aws:s3:::{STORAGE_BUCKET_NAME}"

S3_BATCH_COPY_ROLE_ARN = get_param(ParameterName.PROCESSING_IMPORT_DATASET_ROLE_ARN)

EVENT_KEY = "event"


class Importer:
    # pylint:disable=too-few-public-methods
    def __init__(self, dataset_id: str, version_id: str, source_bucket_name: str):
        self.dataset_id = dataset_id
        self.version_id = version_id
        self.source_bucket_name = source_bucket_name

    def run(self, task_arn: str, processing_asset_type: ProcessingAssetType) -> str:
        manifest_key = f"manifests/{self.version_id}_{processing_asset_type.value}.csv"
        with smart_open(f"s3://{STORAGE_BUCKET_NAME}/{manifest_key}", "w") as s3_manifest:
            processing_assets_model = processing_assets_model_with_meta()

            for item in processing_assets_model.query(
                f"DATASET#{self.dataset_id}#VERSION#{self.version_id}",
                range_key_condition=processing_assets_model.sk.startswith(
                    f"{processing_asset_type.value}#"
                ),
            ):
                LOGGER.debug(dumps({"Adding file to manifest": item.url}))
                key = s3_url_to_key(item.url)
                task_parameters = {
                    TARGET_BUCKET_NAME_KEY: STORAGE_BUCKET_NAME,
                    ORIGINAL_KEY_KEY: key,
                    NEW_KEY_KEY: f"{self.dataset_id}/{self.version_id}/{basename(key)}",
                }
                row = ",".join([self.source_bucket_name, quote(dumps(task_parameters))])
                s3_manifest.write(f"{row}\n")

        manifest_s3_object = S3_CLIENT.head_object(Bucket=STORAGE_BUCKET_NAME, Key=manifest_key)
        assert "ETag" in manifest_s3_object, manifest_s3_object
        manifest_s3_etag = manifest_s3_object["ETag"]
        manifest_location_spec = JobManifestLocationTypeDef(
            ObjectArn=f"{STORAGE_BUCKET_ARN}/{manifest_key}", ETag=manifest_s3_etag
        )

        account_number = get_account_number()

        # trigger s3 batch copy operation
        response = S3CONTROL_CLIENT.create_job(
            AccountId=account_number,
            ConfirmationRequired=False,
            Operation=JobOperationTypeDef(
                LambdaInvoke=LambdaInvokeOperationTypeDef(FunctionArn=task_arn)
            ),
            Manifest=JobManifestTypeDef(
                Spec=JobManifestSpecTypeDef(
                    Format="S3BatchOperations_CSV_20180820", Fields=["Bucket", "Key"]
                ),
                Location=manifest_location_spec,
            ),
            Report=JobReportTypeDef(
                Enabled=True,
                Bucket=STORAGE_BUCKET_ARN,
                Format="Report_CSV_20180820",
                Prefix=f"reports/{self.version_id}",
                ReportScope="AllTasks",
            ),
            Priority=1,
            RoleArn=S3_BATCH_COPY_ROLE_ARN,
            ClientRequestToken=uuid4().hex,
        )
        LOGGER.debug(dumps({"s3 batch response": response}, default=str))

        return response["JobId"]


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    """Main Lambda entry point."""
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

    source_bucket_name = urlparse(event[METADATA_URL_KEY]).netloc

    importer = Importer(event[DATASET_ID_KEY], event[VERSION_ID_KEY], source_bucket_name)
    asset_job_id = importer.run(IMPORT_ASSET_FILE_TASK_ARN, ProcessingAssetType.DATA)
    metadata_job_id = importer.run(IMPORT_METADATA_FILE_TASK_ARN, ProcessingAssetType.METADATA)

    return {ASSET_JOB_ID_KEY: asset_job_id, METADATA_JOB_ID_KEY: metadata_job_id}


@lru_cache
def get_account_number() -> str:
    caller_identity = STS_CLIENT.get_caller_identity()
    assert "Account" in caller_identity, caller_identity
    return caller_identity["Account"]


def s3_url_to_key(url: str) -> str:
    return urlparse(url).path[1:]
