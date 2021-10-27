from enum import Enum
from json import dumps, loads
from typing import TYPE_CHECKING, Optional

import boto3
from linz_logger import get_log

from .api_keys import SUCCESS_KEY
from .boto3_config import CONFIG
from .import_file_batch_job_id_keys import ASSET_JOB_ID_KEY, METADATA_JOB_ID_KEY
from .models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR, VERSION_ID_PREFIX
from .step_function_keys import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_KEY,
    ERRORS_KEY,
    ERROR_CHECK_KEY,
    ERROR_DETAILS_KEY,
    ERROR_RESULT_KEY,
    ERROR_URL_KEY,
    FAILED_TASKS_KEY,
    FAILURE_REASONS_KEY,
    IMPORT_DATASET_KEY,
    JOB_STATUS_RUNNING,
    JOB_STATUS_SUCCEEDED,
    METADATA_UPLOAD_KEY,
    S3_BATCH_RESPONSE_KEY,
    S3_BATCH_STATUS_FAILED,
    STATUS_KEY,
    STEP_FUNCTION_KEY,
    VALIDATION_KEY,
    VERSION_ID_KEY,
)
from .sts import get_account_number
from .types import JsonList, JsonObject
from .validation_results_model import ValidationResult, validation_results_model_with_meta

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_s3control import S3ControlClient
    from mypy_boto3_stepfunctions import SFNClient
else:
    # In production we want to avoid depending on a package which has no runtime impact
    S3ControlClient = SFNClient = object  # pragma: no mutate


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


STEP_FUNCTIONS_CLIENT: SFNClient = boto3.client("stepfunctions", config=CONFIG)
S3CONTROL_CLIENT: S3ControlClient = boto3.client("s3control", config=CONFIG)
LOGGER = get_log()


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


def get_import_status_given_arn(execution_arn_key: str) -> JsonObject:
    step_function_resp = STEP_FUNCTIONS_CLIENT.describe_execution(executionArn=execution_arn_key)
    assert "status" in step_function_resp, step_function_resp
    LOGGER.debug(dumps({"step function response": step_function_resp}, default=str))

    step_function_input = loads(step_function_resp["input"])
    step_function_output = loads(step_function_resp.get("output", "{}"))
    step_function_status = step_function_resp["status"]

    dataset_id = step_function_input[DATASET_ID_KEY]
    version_id = step_function_input[VERSION_ID_KEY]
    validation_success = step_function_output.get(VALIDATION_KEY, {}).get(SUCCESS_KEY)
    import_dataset_jobs = step_function_output.get(IMPORT_DATASET_KEY, {})

    tasks_status = get_tasks_status(
        step_function_status, dataset_id, version_id, validation_success, import_dataset_jobs
    )
    return {STEP_FUNCTION_KEY: {"status": step_function_status.title()}, **tasks_status}


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
    hash_key = get_hash_key(dataset_id, version_id)

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
    account_number = get_account_number()
    s3_batch_copy_resp = S3CONTROL_CLIENT.describe_job(
        AccountId=account_number,
        JobId=s3_batch_copy_job_id,
    )
    assert "Job" in s3_batch_copy_resp, s3_batch_copy_resp
    LOGGER.debug(dumps({S3_BATCH_RESPONSE_KEY: s3_batch_copy_resp}, default=str))

    s3_batch_copy_status = s3_batch_copy_resp["Job"]["Status"]
    failure_reasons = s3_batch_copy_resp["Job"]["FailureReasons"]
    failed_tasks = s3_batch_copy_resp["Job"]["ProgressSummary"]["NumberOfTasksFailed"]

    if failed_tasks > 0:
        s3_batch_copy_status = S3_BATCH_STATUS_FAILED

    return {
        STATUS_KEY: s3_batch_copy_status,
        ERRORS_KEY: {FAILED_TASKS_KEY: failed_tasks, FAILURE_REASONS_KEY: failure_reasons},
    }


def get_hash_key(dataset_id: str, dataset_version_id: str) -> str:
    return (
        f"{DATASET_ID_PREFIX}{dataset_id}{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{dataset_version_id}"
    )
