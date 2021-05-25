import os

import boto3
from pystac import STAC_IO, Catalog, CatalogType, Collection, Item, layout  # type: ignore[import]

from ..api_responses import BODY_KEY
from ..pystac_io_methods import read_method, write_method
from ..resources import ResourceName
from ..s3 import S3_URL_PREFIX
from ..sqs_message_attributes import (
    MESSAGE_ATTRIBUTE_TYPE_DATASET,
    MESSAGE_ATTRIBUTE_TYPE_KEY,
    MESSAGE_ATTRIBUTE_TYPE_ROOT,
    STRING_VALUE_KEY,
)
from ..types import JsonObject

STAC_IO.write_text_method = write_method
STAC_IO.read_text_method = read_method

S3_CLIENT = boto3.client("s3")

ROOT_CATALOG_ID = "root_catalog"
ROOT_CATALOG_TITLE = "Geostore Root Catalog"
ROOT_CATALOG_DESCRIPTION = "The root catalog which links to all dataset catalogues in Geostore"
CATALOG_KEY = "catalog.json"
CONTENTS_KEY = "Contents"
RECORDS_KEY = "Records"
MESSAGE_ATTRIBUTES_KEY = "messageAttributes"


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    """Main Lambda entry point."""
    for message in event[RECORDS_KEY]:
        if (
            message[MESSAGE_ATTRIBUTES_KEY][MESSAGE_ATTRIBUTE_TYPE_KEY][STRING_VALUE_KEY]
            == MESSAGE_ATTRIBUTE_TYPE_ROOT
        ):
            handle_root(message[BODY_KEY])
        elif (
            message[MESSAGE_ATTRIBUTES_KEY][MESSAGE_ATTRIBUTE_TYPE_KEY][STRING_VALUE_KEY]
            == MESSAGE_ATTRIBUTE_TYPE_DATASET
        ):
            handle_dataset(message[BODY_KEY])
        else:
            raise UnhandledSQSMessageException("Unhandled SQS message type")

    return {}


class UnhandledSQSMessageException(Exception):
    pass


def handle_dataset(version_metadata_key: str) -> None:
    """Handle writing a new dataset version to the dataset catalog"""
    storage_bucket_path = f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}"
    dataset_prefix = version_metadata_key.split("/", maxsplit=2)[0]

    dataset_catalog = Catalog.from_file(f"{storage_bucket_path}/{dataset_prefix}/{CATALOG_KEY}")

    dataset_version_metadata = STAC_IO.read_stac_object(
        f"{storage_bucket_path}/{version_metadata_key}"
    )

    dataset_version_strategy = layout.CustomLayoutStrategy(
        catalog_func=get_catalog_href, collection_func=get_collection_href, item_func=get_item_href
    )

    dataset_catalog.add_child(dataset_version_metadata, strategy=dataset_version_strategy)

    dataset_catalog.normalize_hrefs(
        f"{storage_bucket_path}/{dataset_prefix}", strategy=dataset_version_strategy
    )
    dataset_catalog.save(catalog_type=CatalogType.SELF_CONTAINED)


def get_catalog_href(cat: Catalog, parent_dir: str, is_root: bool) -> str:
    og_filename = cat.get_self_href().split("/")
    if is_root:
        cat_root = parent_dir
    else:
        cat_root = os.path.join(parent_dir, "{}".format(og_filename[-2]))

    return os.path.join(cat_root, og_filename[-1])


def get_collection_href(col: Collection, parent_dir: str, is_root: bool) -> str:
    og_filename = col.get_self_href().split("/")
    if is_root:
        col_root = parent_dir
    else:
        col_root = os.path.join(parent_dir, "{}".format(og_filename[-2]))

    return os.path.join(col_root, og_filename[-1])


def get_item_href(item: Item, parent_dir: str) -> str:
    og_filename = item.get_self_href().split("/")
    return os.path.join(parent_dir, "{}.json".format(og_filename[-1]))


def handle_root(dataset_prefix: str) -> None:
    """Handle writing a new dataset to the root catalog"""
    results = S3_CLIENT.list_objects(
        Bucket=ResourceName.STORAGE_BUCKET_NAME.value, Prefix=CATALOG_KEY
    )

    # create root catalog if it doesn't exist
    if CONTENTS_KEY in results:
        root_catalog = Catalog.from_file(
            f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}/{CATALOG_KEY}"
        )

    else:
        root_catalog = Catalog(
            id=ROOT_CATALOG_ID,
            title=ROOT_CATALOG_TITLE,
            description=ROOT_CATALOG_DESCRIPTION,
            catalog_type=CatalogType.SELF_CONTAINED,
        )

    dataset_path = f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}/{dataset_prefix}"
    dataset_catalog = Catalog.from_file(f"{dataset_path}/{CATALOG_KEY}")

    root_catalog.add_child(dataset_catalog)
    root_catalog.normalize_hrefs(f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}")

    root_catalog.save(catalog_type=CatalogType.SELF_CONTAINED)
