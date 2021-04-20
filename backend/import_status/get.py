"""Import Status handler function."""
import json
import logging
from enum import Enum
from http import HTTPStatus
from typing import Optional

import boto3
from jsonschema import ValidationError, validate  # type: ignore[import]

from ..api_responses import error_response, success_response
from ..error_response_keys import ERROR_KEY
from ..import_file_batch_job_id_keys import ASSET_JOB_ID_KEY, METADATA_JOB_ID_KEY
from ..log import set_up_logging
from ..step_function_event_keys import DATASET_ID_KEY, VERSION_ID_KEY
from ..types import JsonList, JsonObject
from ..validation_results_model import ValidationResult, validation_results_model_with_meta

STEP_FUNCTIONS_CLIENT = boto3.client("stepfunctions")
S3CONTROL_CLIENT = boto3.client("s3control")
STS_CLIENT = boto3.client("sts")
LOGGER = set_up_logging(__name__)


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


def get_import_status(event: JsonObject) -> JsonObject:
    LOGGER.debug(json.dumps({"event": event}))

    try:
        validate(
            event["body"],
            {
                "type": "object",
                "properties": {
                    "execution_arn": {"type": "string"},
                },
                "required": ["execution_arn"],
            },
        )
    except ValidationError as err:
        LOGGER.warning(json.dumps({ERROR_KEY: err}, default=str))
        return error_response(HTTPStatus.BAD_REQUEST, err.message)

    step_function_resp = STEP_FUNCTIONS_CLIENT.describe_execution(
        executionArn=event["body"]["execution_arn"]
    )
    assert "status" in step_function_resp, step_function_resp
    LOGGER.debug(json.dumps({"step function response": step_function_resp}, default=str))

    step_function_input = json.loads(step_function_resp["input"])
    step_function_output = json.loads(step_function_resp.get("output", "{}"))
    step_function_status = step_function_resp["status"]

    validation_errors = get_step_function_validation_results(
        step_function_input[DATASET_ID_KEY], step_function_input[VERSION_ID_KEY]
    )

    validation_success = step_function_output.get("validation", {}).get("success")
    validation_outcome = get_validation_outcome(
        step_function_status, validation_errors, validation_success
    )

    metadata_upload_status = get_import_job_status(step_function_output, METADATA_JOB_ID_KEY)
    asset_upload_status = get_import_job_status(step_function_output, ASSET_JOB_ID_KEY)

    # Failed validation implies uploads will never happen
    if (
        metadata_upload_status["status"] == Outcome.PENDING.value
        and asset_upload_status["status"] == Outcome.PENDING.value
        and validation_outcome in [Outcome.FAILED, Outcome.SKIPPED]
    ):
        metadata_upload_status["status"] = asset_upload_status["status"] = Outcome.SKIPPED.value

    response_body = {
        "step function": {"status": step_function_status.title()},
        "validation": {"status": validation_outcome.value, "errors": validation_errors},
        "metadata upload": metadata_upload_status,
        "asset upload": asset_upload_status,
    }

    return success_response(HTTPStatus.OK, response_body)


def get_validation_outcome(
    step_function_status: str, validation_errors: JsonList, validation_success: Optional[bool]
) -> Outcome:
    validation_status = SUCCESS_TO_VALIDATION_OUTCOME_MAPPING[validation_success]
    if validation_status == Outcome.PENDING:
        # Some statuses are not reported by the step function
        if validation_errors:
            validation_status = Outcome.FAILED
        elif step_function_status not in ["RUNNING", "SUCCEEDED"]:
            validation_status = Outcome.SKIPPED
    return validation_status


def get_import_job_status(step_function_output: JsonObject, job_id_key: str) -> JsonObject:
    if s3_job_id := step_function_output.get("import_dataset", {}).get(job_id_key):
        return get_s3_batch_copy_status(s3_job_id, LOGGER)
    return {"status": Outcome.PENDING.value, "errors": []}


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
                "check": check_type,
                "result": validation_item.result,
                "url": url,
                "details": validation_item.details.attribute_values,
            }
        )

    return errors


def get_s3_batch_copy_status(s3_batch_copy_job_id: str, logger: logging.Logger) -> JsonObject:
    caller_identity = STS_CLIENT.get_caller_identity()
    assert "Account" in caller_identity, caller_identity

    s3_batch_copy_resp = S3CONTROL_CLIENT.describe_job(
        AccountId=caller_identity["Account"],
        JobId=s3_batch_copy_job_id,
    )
    assert "Job" in s3_batch_copy_resp, s3_batch_copy_resp
    logger.debug(json.dumps({"s3 batch response": s3_batch_copy_resp}, default=str))

    s3_batch_copy_status = s3_batch_copy_resp["Job"]["Status"]

    upload_errors = s3_batch_copy_resp["Job"].get("FailureReasons", [])

    return {"status": s3_batch_copy_status, "errors": upload_errors}
