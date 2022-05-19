from copy import deepcopy
from json import load

from pytest import mark

from geostore.populate_catalog.task import CATALOG_FILENAME
from geostore.resources import Resource
from geostore.s3 import S3_URL_PREFIX
from geostore.s3_utils import get_s3_url_reader
from geostore.stac_format import (
    STAC_HREF_KEY,
    STAC_LINKS_KEY,
    STAC_REL_KEY,
    STAC_REL_PARENT,
    STAC_REL_ROOT,
)
from tests.aws_utils import S3Object, get_s3_role_arn
from tests.file_utils import json_dict_to_file_object
from tests.general_generators import any_safe_file_path, any_safe_filename
from tests.stac_objects import MINIMAL_VALID_STAC_COLLECTION_OBJECT


@mark.infrastructure
def should_successfully_get_object_from_staging_bucket() -> None:
    key_prefix = any_safe_file_path()
    metadata_url_prefix = (
        f"{S3_URL_PREFIX}{Resource.STAGING_BUCKET_NAME.resource_name}/{key_prefix}"
    )
    collection_metadata_filename = any_safe_filename()
    collection_metadata_url = f"{metadata_url_prefix}/{collection_metadata_filename}"

    collection_dict = {
        **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
        STAC_LINKS_KEY: [
            {STAC_HREF_KEY: f"../{CATALOG_FILENAME}", STAC_REL_KEY: STAC_REL_ROOT},
            {
                STAC_HREF_KEY: f"../{CATALOG_FILENAME}",
                STAC_REL_KEY: STAC_REL_PARENT,
            },
        ],
    }

    with S3Object(
        file_object=json_dict_to_file_object(collection_dict),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{collection_metadata_filename}",
    ):

        s3_url_reader = get_s3_url_reader(get_s3_role_arn())
        json_object = load(s3_url_reader(collection_metadata_url))

        assert json_object == collection_dict
