"""Import Status handler function."""
import json
import logging

import boto3
from jsonschema import ValidationError, validate  # type: ignore[import]

from ..api_responses import error_response, success_response
from ..log import set_up_logging
from ..types import JsonObject

STEP_FUNCTIONS_CLIENT = boto3.client("stepfunctions")
S3CONTROL_CLIENT = boto3.client("s3control")
STS_CLIENT = boto3.client("sts")
LOGGER = set_up_logging(__name__)


def get_import_status(payload: JsonObject) -> JsonObject:

    LOGGER.debug(json.dumps({"payload": payload}))

    try:
        validate(
            payload["body"],
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
        executionArn=payload["body"]["execution_arn"]
    )
    assert "status" in step_function_resp, step_function_resp
    LOGGER.debug(json.dumps({"step function response": step_function_resp}, default=str))

    upload_response: JsonObject = {"status": "Pending", "errors": []}

    # only check status of upload if step function has completed
    if step_function_resp["status"] == "SUCCEEDED":
        assert "output" in step_function_resp, step_function_resp
        step_functions_output = json.loads(step_function_resp["output"])

        assert (
            "s3_batch_copy" in step_functions_output
            and "job_id" in step_functions_output["s3_batch_copy"]
        ), step_function_resp

        upload_response = get_s3_batch_copy_status(
            step_functions_output["s3_batch_copy"]["job_id"], LOGGER
        )

    response_body = {
        "validation": {"status": step_function_resp["status"]},
        "upload": upload_response,
    }

    return success_response(200, response_body)


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
