from copy import deepcopy
from json import load
from os.path import basename
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

from botocore.exceptions import ClientError
from linz_logger import get_log
from pytest import mark
from pytest_subtests import SubTests

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
from tests.aws_utils import Dataset, S3Object, any_operation_name, any_s3_url, get_s3_role_arn
from tests.file_utils import json_dict_to_file_object
from tests.general_generators import any_error_message, any_safe_file_path, any_safe_filename
from tests.stac_generators import any_dataset_prefix
from tests.stac_objects import MINIMAL_VALID_STAC_COLLECTION_OBJECT

if TYPE_CHECKING:
    from botocore.exceptions import ClientErrorResponseError, ClientErrorResponseTypeDef
else:
    ClientErrorResponseError = ClientErrorResponseTypeDef = dict


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

        s3_url_reader = get_s3_url_reader(get_s3_role_arn(), any_dataset_prefix(), get_log())
        json_object = load(s3_url_reader(collection_metadata_url).response)

        assert json_object == collection_dict


@mark.infrastructure
def should_get_object_from_storage_bucket_when_staging_bucket_fails() -> None:
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

    with Dataset() as dataset, S3Object(
        file_object=json_dict_to_file_object(collection_dict),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=f"{dataset.dataset_prefix}/{collection_metadata_filename}",
    ):

        s3_url_reader = get_s3_url_reader(get_s3_role_arn(), dataset.dataset_prefix, get_log())
        json_object = load(s3_url_reader(collection_metadata_url).response)

        assert json_object == collection_dict


@patch("geostore.s3_utils.get_s3_client_for_role")
def should_log_message_when_using_geostore_file_for_validation(
    get_s3_client_for_role_mock: MagicMock, subtests: SubTests
) -> None:
    # Given

    s3_url = any_s3_url()
    dataset_prefix = any_dataset_prefix()

    expected_staging_key = urlparse(s3_url).path[1:]
    expected_geostore_key = f"{dataset_prefix}/{basename(expected_staging_key)}"

    operation_name = any_operation_name()
    error_message = any_error_message()
    error = ClientError(
        ClientErrorResponseTypeDef(
            Error=ClientErrorResponseError(Code="NoSuchKey", Message=error_message)
        ),
        operation_name,
    )
    get_s3_client_for_role_mock.return_value.get_object.side_effect = [error, {}]

    expected_message = (
        f"'{expected_staging_key}' is not present in the staging bucket."
        f" Using '{expected_geostore_key}' from the geostore bucket for validation instead."
    )

    logger_mock = MagicMock()

    # When
    s3_url_reader = get_s3_url_reader(get_s3_role_arn(), dataset_prefix, logger_mock)
    s3_url_reader(s3_url)

    # Then
    with subtests.test(msg="log"):
        logger_mock.debug.assert_any_call(expected_message)
