"""Create dataset function."""
from http import HTTPStatus
from string import ascii_letters, digits

from jsonschema import ValidationError, validate  # type: ignore[import]
from pystac import STAC_IO, Catalog, CatalogType  # type: ignore[import]

from ..api_responses import error_response, success_response
from ..datasets_model import datasets_model_with_meta
from ..pystac_io_methods import write_method
from ..resources import ResourceName
from ..types import JsonObject

TITLE_CHARACTERS = f"{ascii_letters}{digits}_-"
TITLE_PATTERN = f"^[{TITLE_CHARACTERS}]+$"
STAC_IO.write_text_method = write_method


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
    if datasets_model_class.datasets_title_idx.count(hash_key=body["title"]):
        return error_response(HTTPStatus.CONFLICT, f"dataset '{body['title']}' already exists")

    # create dataset
    dataset = datasets_model_class(title=body["title"])
    dataset.save()
    dataset.refresh(consistent_read=True)

    # create dataset catalog
    dataset_catalog = Catalog(
        id=dataset.dataset_id, description=body["description"], title=body["title"]
    )
    dataset_catalog.normalize_hrefs(
        f"s3://{ResourceName.STORAGE_BUCKET_NAME.value}/{dataset.dataset_prefix}"
    )
    dataset_catalog.save(catalog_type=CatalogType.SELF_CONTAINED)

    # return response
    resp_body = dataset.as_dict()

    return success_response(HTTPStatus.CREATED, resp_body)
