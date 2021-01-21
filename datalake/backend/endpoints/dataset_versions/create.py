"""Import dataset-version function."""
import json
import logging
import uuid
from os import environ

import boto3
from jsonschema import ValidationError, validate  # type: ignore[import]
from pynamodb.exceptions import DoesNotExist

from ..datasets.model import DatasetModel
from ..utils import DATASET_TYPES, ENV, JSON_OBJECT, error_response, success_response

stepfunc_client = boto3.client("stepfunctions")
ssm_client = boto3.client("ssm")

STEP_FUNCTION_ARN_PARAMETER_NAME = f"/{ENV}/StepFuncStateMachineARN"


def create_dataset_version(payload: JSON_OBJECT) -> JSON_OBJECT:
    """POST: Create Dataset."""

    logger = set_up_logging()

    logger.debug(json.dumps({"payload": payload}))

    BODY_SCHEMA = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "type": {
                "type": "string",
                "enum": DATASET_TYPES,
            },
            "metadata-url": {"type": "string"},
        },
        "required": ["id", "metadata-url", "type"],
    }

    # validate input
    req_body = payload["body"]
    try:
        validate(req_body, BODY_SCHEMA)
    except ValidationError as err:
        logger.warning(json.dumps({"error": err}, default=str))
        return error_response(400, err.message)

    # validate dataset exists
    try:
        dataset = DatasetModel.get(
            hash_key=f"DATASET#{req_body['id']}",
            range_key=f"TYPE#{req_body['type']}",
            consistent_read=True,
        )
    except DoesNotExist as err:
        logger.warning(json.dumps({"error": err}, default=str))
        return error_response(404, f"dataset '{req_body['id']}' could not be found")

    transaction_id = str(uuid.uuid1())

    # execute step function
    stepfunction_input = {
        "dataset_id": dataset.dataset_id,
        "version_id": transaction_id,
        "type": dataset.dataset_type,
        "metadata_url": req_body["metadata-url"],
    }
    stepfunc_arn = get_param(STEP_FUNCTION_ARN_PARAMETER_NAME)

    stepfunc_resp = stepfunc_client.start_execution(
        stateMachineArn=stepfunc_arn,
        name=transaction_id,
        input=json.dumps(stepfunction_input),
    )

    logger.debug(json.dumps({"response": stepfunc_resp}, default=str))

    # return arn of executing process
    return success_response(
        201, {"dataset_version": transaction_id, "execution_arn": stepfunc_resp["executionArn"]}
    )


def get_param(parameter: str) -> str:
    """Retrieve ARN of State Function Machine"""
    parameter_response = ssm_client.get_parameter(Name=parameter)

    try:
        parameter = parameter_response["Parameter"]["Value"]
    except KeyError:
        print(parameter_response)
        raise

    return parameter


def set_up_logging() -> logging.Logger:
    logger = logging.getLogger(__name__)

    log_handler = logging.StreamHandler()
    log_level = environ.get("LOGLEVEL", logging.NOTSET)

    logger.addHandler(log_handler)
    logger.setLevel(log_level)

    return logger
