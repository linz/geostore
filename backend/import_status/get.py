"""Import Status handler function."""
import json
import logging
from enum import Enum

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


class ValidationOutcome(Enum):
    PASSED = "Passed"
    PENDING = "Pending"
    FAILED = "Failed"


SUCCESS_TO_VALIDATION_OUTCOME_MAPPING = {
    True: ValidationOutcome.PASSED.value,
    False: ValidationOutcome.FAILED.value,
    None: ValidationOutcome.PENDING.value,
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
        return error_response(400, err.message)

    step_function_resp = STEP_FUNCTIONS_CLIENT.describe_execution(
        executionArn=event["body"]["execution_arn"]
    )
    assert "status" in step_function_resp, step_function_resp
    LOGGER.debug(json.dumps({"step function response": step_function_resp}, default=str))

    step_function_input = json.loads(step_function_resp["input"])
    step_function_output = json.loads(step_function_resp.get("output", "{}"))

    validation_status = SUCCESS_TO_VALIDATION_OUTCOME_MAPPING.get(
        step_function_output.get("validation", {}).get("success")
    )

    response_body = {
        "step function": {"status": step_function_resp["status"]},
        "validation": {
            "status": validation_status,
            "errors": get_step_function_validation_results(
                step_function_input[DATASET_ID_KEY], step_function_input[VERSION_ID_KEY]
            ),
        },
        "metadata upload": get_import_job_status(step_function_output, METADATA_JOB_ID_KEY),
        "asset upload": get_import_job_status(step_function_output, ASSET_JOB_ID_KEY),
    }

    return success_response(200, response_body)


def get_import_job_status(step_function_output: JsonObject, job_id_key: str) -> JsonObject:
    if s3_job_id := step_function_output.get("import_dataset", {}).get(job_id_key):
        return get_s3_batch_copy_status(s3_job_id, LOGGER)
    return {"status": "Pending", "errors": []}


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
