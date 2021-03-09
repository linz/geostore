"""Dataset versions handler function."""
import json

import boto3
from jsonschema import ValidationError, validate  # type: ignore[import]

from ..log import set_up_logging
from ..api_responses import success_response, error_response, JsonObject

STEPFUNCTIONS_CLIENT = boto3.client("stepfunctions")
S3CONTROL_CLIENT = boto3.client("s3control")
STS_CLIENT = boto3.client("sts")


def get_import_status(payload: JsonObject) -> JsonObject:
    logger = set_up_logging(__name__)

    logger.debug(json.dumps({"payload": payload}))

    # validate input
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
        logger.warning(json.dumps({"error": err}, default=str))
        return error_response(400, err.message)

    step_function_resp = STEPFUNCTIONS_CLIENT.describe_execution(
        executionArn=payload["body"]["execution_arn"]
    )
    assert step_function_resp["status"], step_function_resp

    s3_batch_copy_status = "Pending"
    upload_errors = []

    # if step function has completed then also check status of S3 batch copy operation
    if step_function_resp["status"] == "SUCCEEDED":

        assert step_function_resp["output"], step_function_resp
        s3_batch_copy_arn = json.loads(step_function_resp["output"])["s3_batch_copy"]["job_id"]

        s3_batch_copy_resp = S3CONTROL_CLIENT.describe_job(
            AccountId=STS_CLIENT.get_caller_identity()["Account"],
            JobId=s3_batch_copy_arn,
        )

        assert s3_batch_copy_resp["Job"]["Status"], s3_batch_copy_resp
        s3_batch_copy_status = s3_batch_copy_resp["Job"]["Status"]

        if "FailureReasons" in s3_batch_copy_resp["Job"]:
            upload_errors = s3_batch_copy_resp["Job"]["FailureReasons"]

    response_body = {
        "validation": {"status": step_function_resp["status"]},
        "upload": {"status": s3_batch_copy_status, "errors": upload_errors},
    }

    return success_response(200, response_body)
