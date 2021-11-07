from json import dumps
from os.path import basename
from typing import TYPE_CHECKING, List
from urllib.parse import quote, urlparse
from uuid import uuid4

import boto3
import smart_open
from jsonschema import ValidationError, validate
from linz_logger import get_log

from ..api_keys import EVENT_KEY
from ..boto3_config import CONFIG
from ..error_response_keys import ERROR_KEY, ERROR_MESSAGE_KEY
from ..import_dataset_keys import NEW_KEY_KEY, ORIGINAL_KEY_KEY, TARGET_BUCKET_NAME_KEY
from ..import_file_batch_job_id_keys import ASSET_JOB_ID_KEY, METADATA_JOB_ID_KEY
from ..models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR, VERSION_ID_PREFIX
from ..parameter_store import ParameterName, get_param
from ..processing_assets_model import ProcessingAssetType, processing_assets_model_with_meta
from ..resources import Resource
from ..s3 import S3_URL_PREFIX
from ..s3_utils import get_bucket_and_key_from_url
from ..step_function_keys import (
    DATASET_ID_KEY,
    DATASET_PREFIX_KEY,
    METADATA_URL_KEY,
    S3_BATCH_RESPONSE_KEY,
    S3_ROLE_ARN_KEY,
    VERSION_ID_KEY,
)
from ..sts import get_account_number
from ..types import JsonObject

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3control import S3ControlClient
    from mypy_boto3_s3control.literals import (
        JobManifestFieldNameType,
        JobManifestFormatType,
        JobReportFormatType,
        JobReportScopeType,
    )
    from mypy_boto3_s3control.type_defs import (
        JobManifestLocationTypeDef,
        JobManifestSpecTypeDef,
        JobManifestTypeDef,
        JobOperationTypeDef,
        JobReportTypeDef,
        LambdaInvokeOperationTypeDef,
    )
else:
    JobManifestLocationTypeDef = (
        JobManifestSpecTypeDef
    ) = (
        JobManifestTypeDef
    ) = (
        JobOperationTypeDef
    ) = JobReportTypeDef = LambdaInvokeOperationTypeDef = dict  # pragma: no mutate
    JobManifestFieldNameType = (
        JobManifestFormatType
    ) = JobReportFormatType = JobReportScopeType = str  # pragma: no mutate
    S3Client = S3ControlClient = object  # pragma: no mutate

LOGGER = get_log()

S3_CLIENT: S3Client = boto3.client("s3", config=CONFIG)
S3CONTROL_CLIENT: S3ControlClient = boto3.client("s3control", config=CONFIG)

IMPORT_ASSET_FILE_TASK_ARN = get_param(ParameterName.PROCESSING_IMPORT_ASSET_FILE_FUNCTION_TASK_ARN)
IMPORT_METADATA_FILE_TASK_ARN = get_param(
    ParameterName.PROCESSING_IMPORT_METADATA_FILE_FUNCTION_TASK_ARN
)

STORAGE_BUCKET_ARN = f"arn:aws:s3:::{Resource.STORAGE_BUCKET_NAME.resource_name}"

S3_BATCH_COPY_ROLE_ARN = get_param(ParameterName.PROCESSING_IMPORT_DATASET_ROLE_ARN)

JOB_MANIFEST_FORMAT: JobManifestFormatType = "S3BatchOperations_CSV_20180820"
JOB_MANIFEST_FIELD_NAMES: List[JobManifestFieldNameType] = ["Bucket", "Key"]
JOB_REPORT_FORMAT: JobReportFormatType = "Report_CSV_20180820"
JOB_REPORT_SCOPE: JobReportScopeType = "AllTasks"


class Importer:
    # pylint:disable=too-few-public-methods
    def __init__(  # pylint:disable=too-many-arguments
        self,
        dataset_id: str,
        dataset_prefix: str,
        version_id: str,
        source_bucket_name: str,
        s3_role_arn: str,
    ):
        self.dataset_id = dataset_id
        self.version_id = version_id
        self.source_bucket_name = source_bucket_name
        self.dataset_prefix = dataset_prefix
        self.s3_role_arn = s3_role_arn

    def run(self, task_arn: str, processing_asset_type: ProcessingAssetType) -> str:
        manifest_key = f"manifests/{self.version_id}_{processing_asset_type.value}.csv"
        with smart_open.open(
            f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}/{manifest_key}", "w"
        ) as s3_manifest:
            processing_assets_model = processing_assets_model_with_meta()

            for item in processing_assets_model.query(
                (
                    f"{DATASET_ID_PREFIX}{self.dataset_id}"
                    f"{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{self.version_id}"
                ),
                range_key_condition=processing_assets_model.sk.startswith(
                    f"{processing_asset_type.value}{DB_KEY_SEPARATOR}"
                ),
                consistent_read=True,
            ):
                LOGGER.debug(dumps({"Adding file to manifest": item.url}))
                _, key = get_bucket_and_key_from_url(item.url)
                task_parameters = {
                    TARGET_BUCKET_NAME_KEY: Resource.STORAGE_BUCKET_NAME.resource_name,
                    ORIGINAL_KEY_KEY: key,
                    NEW_KEY_KEY: f"{self.dataset_prefix}/{self.version_id}/{basename(key)}",
                    S3_ROLE_ARN_KEY: self.s3_role_arn,
                }
                row = ",".join([self.source_bucket_name, quote(dumps(task_parameters))])
                s3_manifest.write(f"{row}\n")

        manifest_s3_object = S3_CLIENT.head_object(
            Bucket=Resource.STORAGE_BUCKET_NAME.resource_name, Key=manifest_key
        )
        assert "ETag" in manifest_s3_object, manifest_s3_object
        manifest_location_spec = JobManifestLocationTypeDef(
            ObjectArn=f"{STORAGE_BUCKET_ARN}/{manifest_key}", ETag=manifest_s3_object["ETag"]
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
                    Format=JOB_MANIFEST_FORMAT, Fields=JOB_MANIFEST_FIELD_NAMES
                ),
                Location=manifest_location_spec,
            ),
            Report=JobReportTypeDef(
                Enabled=True,
                Bucket=STORAGE_BUCKET_ARN,
                Format=JOB_REPORT_FORMAT,
                Prefix=f"reports/{self.version_id}",
                ReportScope=JOB_REPORT_SCOPE,
            ),
            Priority=1,
            RoleArn=S3_BATCH_COPY_ROLE_ARN,
            ClientRequestToken=uuid4().hex,
        )
        LOGGER.debug(dumps({S3_BATCH_RESPONSE_KEY: response}, default=str))

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
                    DATASET_PREFIX_KEY: {"type": "string"},
                    VERSION_ID_KEY: {"type": "string"},
                    METADATA_URL_KEY: {"type": "string"},
                    S3_ROLE_ARN_KEY: {"type": "string"},
                },
                "required": [
                    DATASET_ID_KEY,
                    DATASET_PREFIX_KEY,
                    METADATA_URL_KEY,
                    S3_ROLE_ARN_KEY,
                    VERSION_ID_KEY,
                ],
            },
        )
    except ValidationError as error:
        LOGGER.warning(dumps({ERROR_KEY: error}, default=str))
        return {ERROR_MESSAGE_KEY: error.message}

    source_bucket_name = urlparse(event[METADATA_URL_KEY]).netloc

    importer = Importer(
        event[DATASET_ID_KEY],
        event[DATASET_PREFIX_KEY],
        event[VERSION_ID_KEY],
        source_bucket_name,
        event[S3_ROLE_ARN_KEY],
    )
    asset_job_id = importer.run(IMPORT_ASSET_FILE_TASK_ARN, ProcessingAssetType.DATA)
    metadata_job_id = importer.run(IMPORT_METADATA_FILE_TASK_ARN, ProcessingAssetType.METADATA)

    return {ASSET_JOB_ID_KEY: asset_job_id, METADATA_JOB_ID_KEY: metadata_job_id}
