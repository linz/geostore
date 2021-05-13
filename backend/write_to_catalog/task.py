import boto3
from pystac import STAC_IO, Catalog, CatalogType  # type: ignore[import]

from ..pystac_io_methods import read_method, write_method
from ..resources import ResourceName
from ..types import JsonObject

STAC_IO.write_text_method = write_method
STAC_IO.read_text_method = read_method

S3_CLIENT = boto3.client("s3")


def lambda_handler(_event: JsonObject, _context: bytes) -> JsonObject:
    """Main Lambda entry point."""

    results = S3_CLIENT.list_objects(
        Bucket=ResourceName.STORAGE_BUCKET_NAME.value, Prefix="catalog.json"
    )

    # create root catalog if it doesn't exist
    if "Contents" not in results:
        root_catalog = Catalog(
            id="root_catalog_id",
            title="Geospatial Datalake Root Catalog",
            description="The root catalog which links to all datasets catalogs in the data lake",
            catalog_type=CatalogType.SELF_CONTAINED,
        )
        root_catalog.normalize_hrefs(f"s3://{ResourceName.STORAGE_BUCKET_NAME.value}")

    else:
        root_catalog = Catalog.from_file(
            f"s3://{ResourceName.STORAGE_BUCKET_NAME.value}/catalog.json"
        )

    for record in _event["Records"]:
        dataset_title = record["body"]["dataset_title"]
        dataset_catalog = Catalog.from_file(
            f"s3://{ResourceName.STORAGE_BUCKET_NAME.value}/{dataset_title}/catalog.json"
        )
        root_catalog.add_child(dataset_catalog)
        root_catalog.save()
        dataset_catalog.save()

    return {}
