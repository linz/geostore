from enum import Enum
from json import dumps
from typing import TYPE_CHECKING, Optional

import boto3

from .import_file_batch_job_id_keys import ASSET_JOB_ID_KEY, METADATA_JOB_ID_KEY
from .log import set_up_logging
from .types import JsonList, JsonObject
from .validation_results_model import ValidationResult, validation_results_model_with_meta

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_s3control import S3ControlClient
    from mypy_boto3_sts import STSClient
else:
    # In production we want to avoid depending on a package which has no runtime impact
    S3ControlClient = object
    STSClient = object

JOB_STATUS_RUNNING = "RUNNING"
JOB_STATUS_SUCCEEDED = "SUCCEEDED"

ASSET_UPLOAD_KEY = "asset_upload"
DATASET_ID_KEY = "dataset_id"
DATASET_ID_SHORT_KEY = "id"
DATASET_PREFIX_KEY = "dataset_prefix"
DESCRIPTION_KEY = "description"
ERRORS_KEY = "errors"
ERROR_CHECK_KEY = "check"
ERROR_DETAILS_KEY = "details"
ERROR_RESULT_KEY = "result"
ERROR_URL_KEY = "url"
EXECUTION_ARN_KEY = "execution_arn"
IMPORT_DATASET_KEY = "import_dataset"
METADATA_UPLOAD_KEY = "metadata_upload"
METADATA_URL_KEY = "metadata_url"
NOW_KEY = "now"
S3_BATCH_RESPONSE_KEY = "s3_batch_response"
STATUS_KEY = "status"
STEP_FUNCTION_KEY = "step_function"
TITLE_KEY = "title"
VALIDATION_KEY = "validation"
VERSION_ID_KEY = "version_id"


class Outcome(Enum):
    PASSED = "Passed"
    PENDING = "Pending"
    FAILED = "Failed"
    SKIPPED = "Skipped"


SUCCESS_TO_VALIDATION_OUTCOME_MAPPING = {
    True: Outcome.PASSED,
    False: Outcome.FAILED,
    None: Outcome.PENDING,
}

S3CONTROL_CLIENT: S3ControlClient = boto3.client("s3control")
STS_CLIENT: STSClient = boto3.client("sts")
LOGGER = set_up_logging(__name__)


def get_tasks_status(
    step_function_status: str,
    dataset_id: str,
    version_id: str,
    validation_success: Optional[bool],
    import_dataset_jobs: JsonObject,
) -> JsonObject:
    validation_errors = get_step_function_validation_results(dataset_id, version_id)
    validation_outcome = get_validation_outcome(
        step_function_status, validation_errors, validation_success
    )

    metadata_upload_status = get_import_job_status(import_dataset_jobs, METADATA_JOB_ID_KEY)
    asset_upload_status = get_import_job_status(import_dataset_jobs, ASSET_JOB_ID_KEY)

    # Failed validation implies uploads will never happen
    if (
        metadata_upload_status[STATUS_KEY] == Outcome.PENDING.value
        and asset_upload_status[STATUS_KEY] == Outcome.PENDING.value
        and validation_outcome in [Outcome.FAILED, Outcome.SKIPPED]
    ):
        metadata_upload_status[STATUS_KEY] = asset_upload_status[STATUS_KEY] = Outcome.SKIPPED.value

    return {
        VALIDATION_KEY: {STATUS_KEY: validation_outcome.value, ERRORS_KEY: validation_errors},
        METADATA_UPLOAD_KEY: metadata_upload_status,
        ASSET_UPLOAD_KEY: asset_upload_status,
    }


def get_validation_outcome(
    step_function_status: str, validation_errors: JsonList, validation_success: Optional[bool]
) -> Outcome:
    validation_status = SUCCESS_TO_VALIDATION_OUTCOME_MAPPING[validation_success]
    if validation_status == Outcome.PENDING:
        # Some statuses are not reported by the step function
        if validation_errors:
            validation_status = Outcome.FAILED
        elif step_function_status not in [JOB_STATUS_RUNNING, JOB_STATUS_SUCCEEDED]:
            validation_status = Outcome.SKIPPED
    return validation_status


def get_import_job_status(import_dataset_jobs: JsonObject, job_id_key: str) -> JsonObject:
    if s3_job_id := import_dataset_jobs.get(job_id_key):
        return get_s3_batch_copy_status(s3_job_id)
    return {STATUS_KEY: Outcome.PENDING.value, ERRORS_KEY: []}


def get_step_function_validation_results(dataset_id: str, version_id: str) -> JsonList:
    hash_key = f"DATASET#{dataset_id}#VERSION#{version_id}"

    errors = []
    validation_results_model = validation_results_model_with_meta()
    for (
        validation_item
    ) in validation_results_model.validation_outcome_index.query(  # pylint: disable=no-member
        hash_key=hash_key,
        range_key_condition=validation_results_model.result == ValidationResult.FAILED.value,
    ):
        _, check_type, _, url = validation_item.sk.split("#", maxsplit=4)
        errors.append(
            {
                ERROR_CHECK_KEY: check_type,
                ERROR_RESULT_KEY: validation_item.result,
                ERROR_URL_KEY: url,
                ERROR_DETAILS_KEY: validation_item.details.attribute_values,
            }
        )

    return errors


def get_s3_batch_copy_status(s3_batch_copy_job_id: str) -> JsonObject:
    caller_identity = STS_CLIENT.get_caller_identity()
    assert "Account" in caller_identity, caller_identity

    s3_batch_copy_resp = S3CONTROL_CLIENT.describe_job(
        AccountId=caller_identity["Account"],
        JobId=s3_batch_copy_job_id,
    )
    assert "Job" in s3_batch_copy_resp, s3_batch_copy_resp
    LOGGER.debug(dumps({S3_BATCH_RESPONSE_KEY: s3_batch_copy_resp}, default=str))

    s3_batch_copy_status = s3_batch_copy_resp["Job"]["Status"]

    upload_errors = s3_batch_copy_resp["Job"].get("FailureReasons", [])

    return {STATUS_KEY: s3_batch_copy_status, ERRORS_KEY: upload_errors}
