"""Create dataset function."""
from http import HTTPStatus
from typing import TYPE_CHECKING

import boto3
from jsonschema import ValidationError, validate
from pystac.catalog import Catalog, CatalogType
from pystac.stac_io import StacIO

from ..api_responses import error_response, success_response
from ..aws_message_attributes import (
    DATA_TYPE_STRING,
    MESSAGE_ATTRIBUTE_TYPE_KEY,
    MESSAGE_ATTRIBUTE_TYPE_ROOT,
)
from ..dataset_properties import TITLE_PATTERN
from ..datasets_model import datasets_model_with_meta
from ..parameter_store import ParameterName, get_param
from ..pystac_io_methods import S3StacIO
from ..resources import Resource
from ..s3 import S3_URL_PREFIX
from ..stac_format import STAC_DESCRIPTION_KEY, STAC_ID_KEY, STAC_TITLE_KEY
from ..step_function_keys import DESCRIPTION_KEY, TITLE_KEY
from ..types import JsonObject

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_sqs import SQSServiceResource
    from mypy_boto3_sqs.type_defs import MessageAttributeValueTypeDef
else:
    # In production we want to avoid depending on a package which has no runtime impact
    SQSServiceResource = object  # pragma: no mutate
    MessageAttributeValueTypeDef = dict  # pragma: no mutate

SQS_RESOURCE: SQSServiceResource = boto3.resource("sqs")

StacIO.set_default(S3StacIO)


def create_dataset(body: JsonObject) -> JsonObject:
    """POST: Create Dataset."""

    body_schema = {
        "type": "object",
        "properties": {
            TITLE_KEY: {"type": "string", "pattern": TITLE_PATTERN},
            DESCRIPTION_KEY: {"type": "string"},
        },
        "required": [TITLE_KEY, DESCRIPTION_KEY],
    }

    # request body validation
    try:
        validate(body, body_schema)
    except ValidationError as err:
        return error_response(HTTPStatus.BAD_REQUEST, err.message)

    # check for duplicate type/title
    datasets_model_class = datasets_model_with_meta()
    dataset_title = body[TITLE_KEY]
    if datasets_model_class.datasets_title_idx.count(hash_key=dataset_title):
        return error_response(HTTPStatus.CONFLICT, f"dataset '{dataset_title}' already exists")

    # create dataset
    dataset = datasets_model_class(title=dataset_title)
    dataset.save()
    dataset.refresh(consistent_read=True)

    # create dataset catalog
    dataset_catalog = Catalog(
        **{
            STAC_ID_KEY: dataset.dataset_prefix,
            STAC_DESCRIPTION_KEY: body[DESCRIPTION_KEY],
            STAC_TITLE_KEY: dataset_title,
        },
        catalog_type=CatalogType.SELF_CONTAINED,
    )
    dataset_catalog.normalize_hrefs(
        f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}/{dataset.dataset_prefix}"
    )
    dataset_catalog.save()

    # add reference to root catalog
    SQS_RESOURCE.get_queue_by_name(
        QueueName=get_param(ParameterName.UPDATE_CATALOG_MESSAGE_QUEUE_NAME)
    ).send_message(
        MessageBody=dataset.dataset_prefix,
        MessageAttributes={
            MESSAGE_ATTRIBUTE_TYPE_KEY: MessageAttributeValueTypeDef(
                DataType=DATA_TYPE_STRING, StringValue=MESSAGE_ATTRIBUTE_TYPE_ROOT
            )
        },
    )

    # return response
    resp_body = dataset.as_dict()

    return success_response(HTTPStatus.CREATED, resp_body)
