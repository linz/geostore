"""Delete dataset function."""
from http import HTTPStatus
from typing import TYPE_CHECKING

import boto3
from jsonschema import ValidationError, validate
from pynamodb.exceptions import DoesNotExist

from ..api_responses import error_response, success_response
from ..boto3_config import CONFIG
from ..datasets_model import datasets_model_with_meta
from ..models import DATASET_ID_PREFIX
from ..resources import Resource
from ..step_function_keys import DATASET_ID_SHORT_KEY
from ..types import JsonObject

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_s3 import S3Client
else:
    # In production we want to avoid depending on a package which has no runtime impact
    S3Client = object  # pragma: no mutate

S3_CLIENT: S3Client = boto3.client("s3", config=CONFIG)


def delete_dataset(body: JsonObject) -> JsonObject:
    """DELETE: Delete Dataset."""

    body_schema = {
        "type": "object",
        "properties": {DATASET_ID_SHORT_KEY: {"type": "string"}},
        "required": [DATASET_ID_SHORT_KEY],
    }

    # request body validation
    try:
        validate(body, body_schema)
    except ValidationError as err:
        return error_response(HTTPStatus.BAD_REQUEST, err.message)

    datasets_model_class = datasets_model_with_meta()

    # get dataset to delete
    dataset_id = body[DATASET_ID_SHORT_KEY]
    try:
        dataset = datasets_model_class.get(
            hash_key=f"{DATASET_ID_PREFIX}{dataset_id}", consistent_read=True
        )
    except DoesNotExist:
        return error_response(HTTPStatus.NOT_FOUND, f"dataset '{dataset_id}' does not exist")

    # Verify that the dataset is empty
    list_objects_response = S3_CLIENT.list_objects_v2(
        Bucket=Resource.STORAGE_BUCKET_NAME.resource_name, MaxKeys=1, Prefix=f"{dataset_id}/"
    )
    if list_objects_response["KeyCount"]:
        return error_response(
            HTTPStatus.CONFLICT,
            f"Can’t delete dataset “{dataset_id}”: dataset versions still exist",
        )

    # delete dataset
    dataset.delete()

    return success_response(HTTPStatus.NO_CONTENT, {})
