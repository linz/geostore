"""Dataset versions handler function."""
import json
import uuid

import boto3
from jsonschema import ValidationError, validate  # type: ignore[import]
from pynamodb.exceptions import DoesNotExist

from ..api_responses import error_response, success_response
from ..dataset import DATASET_TYPES
from ..dataset_model import DatasetModel
from ..environment import ENV
from ..log import set_up_logging
from ..types import JsonObject

STEP_FUNCTIONS_CLIENT = boto3.client("stepfunctions")
SSM_CLIENT = boto3.client("ssm")

DATASET_VERSION_CREATION_STEP_FUNCTION = f"/{ENV}/step-func-statemachine-arn"


def create_dataset_version(event: JsonObject) -> JsonObject:
    logger = set_up_logging(__name__)

    logger.debug(json.dumps({"event": event}))

    body_schema = {
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
    req_body = event["body"]
    try:
        validate(req_body, body_schema)
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

    dataset_version_id = uuid.uuid1().hex

    # execute step function
    step_functions_input = {
        "dataset_id": dataset.dataset_id,
        "version_id": dataset_version_id,
        "type": dataset.dataset_type,
        "metadata_url": req_body["metadata-url"],
    }
    state_machine_arn = get_param(DATASET_VERSION_CREATION_STEP_FUNCTION)

    step_functions_response = STEP_FUNCTIONS_CLIENT.start_execution(
        stateMachineArn=state_machine_arn,
        name=dataset_version_id,
        input=json.dumps(step_functions_input),
    )

    logger.debug(json.dumps({"response": step_functions_response}, default=str))

    # return arn of executing process
    return success_response(
        201,
        {
            "dataset_version": dataset_version_id,
            "execution_arn": step_functions_response["executionArn"],
        },
    )


def get_param(parameter: str) -> str:
    parameter_response = SSM_CLIENT.get_parameter(Name=parameter)

    try:
        parameter = parameter_response["Parameter"]["Value"]
    except KeyError:
        print(parameter_response)
        raise

    return parameter
