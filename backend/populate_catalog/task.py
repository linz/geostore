import boto3
from pystac import STAC_IO, Catalog, CatalogType  # type: ignore[import]

from ..api_responses import BODY_KEY
from ..pystac_io_methods import read_method, write_method
from ..resources import ResourceName
from ..s3 import S3_URL_PREFIX
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


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    """Main Lambda entry point."""

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

    for record in event[RECORDS_KEY]:
        dataset_path = f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}/{record[BODY_KEY]}"
        dataset_catalog = Catalog.from_file(f"{dataset_path}/{CATALOG_KEY}")

        root_catalog.add_child(dataset_catalog)
        root_catalog.normalize_hrefs(f"{S3_URL_PREFIX}{ResourceName.STORAGE_BUCKET_NAME.value}")

        root_catalog.save(catalog_type=CatalogType.SELF_CONTAINED)

    return {}
