from json import dumps
from logging import Logger
from typing import TYPE_CHECKING

import boto3
from linz_logger import get_log
from pystac import read_file
from pystac.catalog import Catalog, CatalogType
from pystac.collection import Collection
from pystac.item import Item
from pystac.layout import HrefLayoutStrategy
from pystac.stac_io import StacIO

from ..api_keys import EVENT_KEY
from ..aws_keys import BODY_KEY
from ..boto3_config import CONFIG
from ..logging_keys import GIT_COMMIT, LOG_MESSAGE_LAMBDA_FAILURE
from ..parameter_store import ParameterName, get_param
from ..pystac_io_methods import S3StacIO
from ..resources import Resource
from ..s3 import S3_URL_PREFIX
from ..types import JsonObject

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_s3 import S3Client
else:
    # In production we want to avoid depending on a package which has no runtime impact
    S3Client = object  # pragma: no mutate

S3_CLIENT: S3Client = boto3.client("s3", config=CONFIG)

ROOT_CATALOG_ID = "root_catalog"
ROOT_CATALOG_TITLE = "LINZ Geostore"
ROOT_CATALOG_DESCRIPTION = (
    "The LINZ Geospatial Data Store (Geostore) contains all the important "
    "geospatial data held by Land Information New Zealand (LINZ).<br/>"
    "Please browse this catalog to find and access our data."
)
CATALOG_FILENAME = "catalog.json"
CONTENTS_KEY = "Contents"
RECORDS_KEY = "Records"

LOGGER: Logger = get_log()

StacIO.set_default(S3StacIO)


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    """Main Lambda entry point."""

    LOGGER.debug(dumps({EVENT_KEY: event, GIT_COMMIT: get_param(ParameterName.GIT_COMMIT)}))

    for message in event[RECORDS_KEY]:
        handle_message(message[BODY_KEY])

    return {}


class GeostoreSTACLayoutStrategy(HrefLayoutStrategy):
    def get_catalog_href(self, cat: Catalog, parent_dir: str, is_root: bool) -> str:
        return str(cat.get_self_href())

    def get_collection_href(self, col: Collection, parent_dir: str, is_root: bool) -> str:
        assert not is_root
        return str(col.get_self_href())

    def get_item_href(self, item: Item, parent_dir: str) -> str:  # pragma: no cover
        raise NotImplementedError()


def handle_message(metadata_key: str) -> None:
    """Handle writing a new dataset to the root catalog"""

    storage_bucket_path = f"{S3_URL_PREFIX}{Resource.STORAGE_BUCKET_NAME.resource_name}"

    # there could be a myriad of problems preventing catalog from being populated
    # hence a rather broad try except exception clause is used
    # an exception thrown here indicates stuck message(s) in the sqs queue
    # logging is monitored by elasticsearch and alerting is set up to notify the team of a problem
    try:
        dataset_metadata = read_file(f"{storage_bucket_path}/{metadata_key}")
        assert isinstance(dataset_metadata, (Catalog, Collection))

        results = S3_CLIENT.list_objects(
            Bucket=Resource.STORAGE_BUCKET_NAME.resource_name, Prefix=CATALOG_FILENAME
        )

        # create root catalog if it doesn't exist
        if CONTENTS_KEY in results:
            root_catalog = Catalog.from_file(f"{storage_bucket_path}/{CATALOG_FILENAME}")

        else:
            root_catalog = Catalog(
                id=ROOT_CATALOG_ID,
                title=ROOT_CATALOG_TITLE,
                description=ROOT_CATALOG_DESCRIPTION,
                catalog_type=CatalogType.SELF_CONTAINED,
            )
            root_catalog.set_self_href(f"{storage_bucket_path}/{CATALOG_FILENAME}")

        if root_catalog.get_child(dataset_metadata.id) is None:
            root_catalog.add_child(child=dataset_metadata, strategy=GeostoreSTACLayoutStrategy())

        root_catalog.save(catalog_type=CatalogType.SELF_CONTAINED)

    except Exception as error:
        LOGGER.warning(
            f"{LOG_MESSAGE_LAMBDA_FAILURE}: Unable to populate catalog due to “{error}”",
            extra={GIT_COMMIT: get_param(ParameterName.GIT_COMMIT)},
        )
        raise
