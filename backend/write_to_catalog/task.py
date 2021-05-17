import boto3
from pystac import STAC_IO, Catalog, CatalogType  # type: ignore[import]

from ..pystac_io_methods import read_method, write_method
from ..resources import ResourceName
from ..types import JsonObject

STAC_IO.write_text_method = write_method
STAC_IO.read_text_method = read_method

S3_CLIENT = boto3.client("s3")

ROOT_CATALOG_ID = "root_catalog_id"
ROOT_CATALOG_TITLE = "Geospatial Datalake Root Catalog"
ROOT_CATALOG_DESCRIPTION = "The root catalog which links to all datasets catalogs in the data lake"
ROOT_CATALOG_KEY = "catalog.json"


def lambda_handler(_event: JsonObject, _context: bytes) -> JsonObject:
    """Main Lambda entry point."""

    results = S3_CLIENT.list_objects(
        Bucket=ResourceName.STORAGE_BUCKET_NAME.value, Prefix=ROOT_CATALOG_KEY
    )

    # create root catalog if it doesn't exist
    if "Contents" not in results:
        root_catalog = Catalog(
            id=ROOT_CATALOG_ID,
            title=ROOT_CATALOG_TITLE,
            description=ROOT_CATALOG_DESCRIPTION,
            catalog_type=CatalogType.SELF_CONTAINED,
        )

    else:
        root_catalog = Catalog.from_file(
            f"s3://{ResourceName.STORAGE_BUCKET_NAME.value}/{ROOT_CATALOG_KEY}"
        )

    for record in _event["Records"]:
        dataset_title_prefix = record["body"]
        dataset_catalog = Catalog.from_file(
            f"s3://{ResourceName.STORAGE_BUCKET_NAME.value}"
            f"/{dataset_title_prefix}/{ROOT_CATALOG_KEY}"
        )

        root_catalog.add_child(dataset_catalog)

        root_catalog.normalize_hrefs(f"s3://{ResourceName.STORAGE_BUCKET_NAME.value}")
        dataset_catalog.normalize_hrefs(
            f"s3://{ResourceName.STORAGE_BUCKET_NAME.value}/{dataset_title_prefix}"
        )

        root_catalog.save()

    return {}
