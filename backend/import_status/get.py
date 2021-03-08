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
    req_body = payload["body"]
    try:
        validate(
            req_body,
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

    execution = STEPFUNCTIONS_CLIENT.describe_execution(executionArn=req_body["execution_arn"])
    assert execution["status"], execution
    validation_status = execution["status"]

    upload_status = "Pending"

    if validation_status == "SUCCEEDED":
        assert execution["output"], execution
        s3_batch_copy_arn = json.loads(execution["output"])["s3_batch_copy"]["job_id"]

        copy_job = S3CONTROL_CLIENT.describe_job(
            AccountId=STS_CLIENT.get_caller_identity()["Account"],
            JobId=s3_batch_copy_arn,
        )

        assert copy_job["Job"]["Status"], copy_job
        upload_status = copy_job["Job"]["Status"]

    response_body = {"validation_status": validation_status, "upload_status": upload_status}

    return success_response(200, response_body)
