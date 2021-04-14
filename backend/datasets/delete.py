"""Delete dataset function."""
from http import HTTPStatus

import boto3
from jsonschema import ValidationError, validate  # type: ignore[import]
from pynamodb.exceptions import DoesNotExist

from ..api_responses import error_response, success_response
from ..datasets_model import datasets_model_with_meta
from ..parameter_store import ParameterName, get_param
from ..types import JsonObject

BOTO3_CLIENT = boto3.client("s3")


def delete_dataset(payload: JsonObject) -> JsonObject:
    """DELETE: Delete Dataset."""

    body_schema = {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}

    # request body validation
    req_body = payload["body"]
    try:
        validate(req_body, body_schema)
    except ValidationError as err:
        return error_response(HTTPStatus.BAD_REQUEST, err.message)

    datasets_model_class = datasets_model_with_meta()

    # get dataset to delete
    dataset_id = req_body["id"]
    try:
        dataset = datasets_model_class.get(hash_key=f"DATASET#{dataset_id}", consistent_read=True)
    except DoesNotExist:
        return error_response(HTTPStatus.NOT_FOUND, f"dataset '{dataset_id}' does not exist")

    # Verify that the dataset is empty
    storage_bucket_name = get_param(ParameterName.STORAGE_BUCKET_NAME)
    list_objects_response = BOTO3_CLIENT.list_objects_v2(
        Bucket=storage_bucket_name, MaxKeys=1, Prefix=f"{dataset_id}/"
    )
    if list_objects_response["KeyCount"]:
        return error_response(
            HTTPStatus.CONFLICT,
            f"Can’t delete dataset “{dataset_id}”: dataset versions still exist",
        )

    # delete dataset
    dataset.delete()

    return success_response(HTTPStatus.NO_CONTENT, {})
