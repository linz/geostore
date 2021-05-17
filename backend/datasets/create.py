"""Create dataset function."""
from http import HTTPStatus
from string import ascii_letters, digits

import boto3
from jsonschema import ValidationError, validate  # type: ignore[import]
from pystac import STAC_IO, Catalog, CatalogType  # type: ignore[import]

from ..api_responses import error_response, success_response
from ..datasets_model import datasets_model_with_meta
from ..parameter_store import ParameterName, get_param
from ..pystac_io_methods import write_method
from ..resources import ResourceName
from ..stac_format import STAC_DESCRIPTION_KEY, STAC_ID_KEY, STAC_TITLE_KEY
from ..types import JsonObject

TITLE_CHARACTERS = f"{ascii_letters}{digits}_-"
TITLE_PATTERN = f"^[{TITLE_CHARACTERS}]+$"
STAC_IO.write_text_method = write_method

SQS_RESOURCE = boto3.resource("sqs")


def create_dataset(body: JsonObject) -> JsonObject:
    """POST: Create Dataset."""

    body_schema = {
        "type": "object",
        "properties": {
            "title": {"type": "string", "pattern": TITLE_PATTERN},
            "description": {"type": "string"},
        },
        "required": ["title", "description"],
    }

    # request body validation
    try:
        validate(body, body_schema)
    except ValidationError as err:
        return error_response(HTTPStatus.BAD_REQUEST, err.message)

    # check for duplicate type/title
    datasets_model_class = datasets_model_with_meta()
    dataset_title = body["title"]
    if datasets_model_class.datasets_title_idx.count(hash_key=dataset_title):
        return error_response(HTTPStatus.CONFLICT, f"dataset '{dataset_title}' already exists")

    # create dataset
    dataset = datasets_model_class(title=dataset_title)
    dataset.save()
    dataset.refresh(consistent_read=True)

    # create dataset catalog
    dataset_catalog = Catalog(
        **{
            STAC_ID_KEY: dataset.dataset_id,
            STAC_DESCRIPTION_KEY: body["description"],
            STAC_TITLE_KEY: dataset_title,
        },
        catalog_type=CatalogType.SELF_CONTAINED,
    )
    dataset_catalog.normalize_hrefs(
        f"s3://{ResourceName.STORAGE_BUCKET_NAME.value}/{dataset.dataset_prefix}"
    )
    dataset_catalog.save()

    # add reference to root catalog
    SQS_RESOURCE.get_queue_by_name(
        QueueName=get_param(ParameterName.ROOT_CATALOG_MESSAGE_QUEUE_NAME)
    ).send_message(MessageBody=dataset.dataset_prefix)

    # return response
    resp_body = dataset.as_dict()

    return success_response(HTTPStatus.CREATED, resp_body)
