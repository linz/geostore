"""Import Status handler function."""
import json
import logging

import boto3
from jsonschema import ValidationError, validate  # type: ignore[import]

from ..api_responses import error_response, success_response
from ..log import set_up_logging
from ..types import JsonList, JsonObject
from ..validation_results_model import ValidationResult, ValidationResultsModel

STEP_FUNCTIONS_CLIENT = boto3.client("stepfunctions")
S3CONTROL_CLIENT = boto3.client("s3control")
STS_CLIENT = boto3.client("sts")
LOGGER = set_up_logging(__name__)


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
        LOGGER.warning(json.dumps({"error": err}, default=str))
        return error_response(400, err.message)

    step_function_resp = STEP_FUNCTIONS_CLIENT.describe_execution(
        executionArn=event["body"]["execution_arn"]
    )
    assert "status" in step_function_resp, step_function_resp
    LOGGER.debug(json.dumps({"step function response": step_function_resp}, default=str))

    step_func_input = json.loads(step_function_resp["input"])
    step_functions_output = json.loads(step_function_resp.get("output", "{}"))

    validation_status = {True: "Passed", False: "Failed", None: "Pending"}.get(
        step_functions_output.get("validation", {}).get("success")
    )

    s3_job_id = step_functions_output.get("s3_batch_copy", {}).get("job_id")
    if s3_job_id:
        upload_response = get_s3_batch_copy_status(s3_job_id, LOGGER)
    else:
        upload_response = {"status": "Pending", "errors": []}

    response_body = {
        "step function": {"status": step_function_resp["status"]},
        "validation": {
            "status": validation_status,
            "errors": get_step_function_validation_results(
                step_func_input["dataset_id"], step_func_input["version_id"]
            ),
        },
        "upload": upload_response,
    }

    return success_response(200, response_body)


def get_step_function_validation_results(dataset_id: str, version_id: str) -> JsonList:
    hash_key = f"DATASET#{dataset_id}#VERSION#{version_id}"

    errors = []
    for validation_item in ValidationResultsModel.validation_outcome_index.query(
        hash_key=hash_key,
        range_key_condition=ValidationResultsModel.result == ValidationResult.FAILED.value,
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
