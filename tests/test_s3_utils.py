from copy import deepcopy
from io import BytesIO
from json import dumps, load
from os.path import basename
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch
from urllib.parse import urlparse

from _pytest.python_api import raises
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from linz_logger import get_log
from mypy_boto3_s3 import S3Client
from pytest import mark
from pytest_subtests import SubTests

from geostore.import_metadata_file.task import S3_BODY_KEY
from geostore.logging_keys import GIT_COMMIT
from geostore.parameter_store import ParameterName, get_param
from geostore.populate_catalog.task import CATALOG_FILENAME
from geostore.pystac_io_methods import S3StacIO
from geostore.resources import Resource
from geostore.s3 import S3_URL_PREFIX
from geostore.s3_utils import calculate_s3_etag, get_s3_etag, get_s3_url_reader
from geostore.stac_format import (
    STAC_HREF_KEY,
    STAC_LINKS_KEY,
    STAC_REL_KEY,
    STAC_REL_PARENT,
    STAC_REL_ROOT,
)
from tests.aws_utils import (
    Dataset,
    S3Object,
    any_error_code,
    any_operation_name,
    any_s3_bucket_name,
    any_s3_url,
    delete_s3_key,
    get_s3_role_arn,
)
from tests.file_utils import json_dict_to_file_object
from tests.general_generators import any_error_message, any_safe_file_path, any_safe_filename
from tests.stac_generators import any_dataset_title
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
            {STAC_HREF_KEY: f"../{CATALOG_FILENAME}", STAC_REL_KEY: STAC_REL_PARENT},
        ],
    }

    with S3Object(
        file_object=json_dict_to_file_object(collection_dict),
        bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{collection_metadata_filename}",
    ):
        s3_url_reader = get_s3_url_reader(get_s3_role_arn(), any_dataset_title(), get_log())
        json_object = load(s3_url_reader(collection_metadata_url).response)

        assert json_object == collection_dict


@mark.infrastructure
def should_get_object_from_storage_bucket_when_not_in_staging_bucket() -> None:
    key_prefix = any_safe_file_path()
    metadata_url_prefix = (
        f"{S3_URL_PREFIX}{Resource.STAGING_BUCKET_NAME.resource_name}/{key_prefix}"
    )
    collection_metadata_filename = any_safe_filename()
    collection_metadata_url = f"{metadata_url_prefix}/{collection_metadata_filename}"

    collection_dict = deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)

    with Dataset() as dataset, S3Object(
        file_object=json_dict_to_file_object(collection_dict),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=f"{dataset.title}/{collection_metadata_filename}",
    ):
        s3_url_reader = get_s3_url_reader(get_s3_role_arn(), dataset.title, get_log())
        json_object = load(s3_url_reader(collection_metadata_url).response)

        assert json_object == collection_dict


@patch("geostore.s3_utils.get_s3_client_for_role")
def should_log_message_when_using_geostore_file_for_validation(
    get_s3_client_for_role_mock: MagicMock, subtests: SubTests
) -> None:
    # Given

    s3_url = any_s3_url()
    dataset_title = any_dataset_title()

    expected_staging_key = urlparse(s3_url).path[1:]
    expected_geostore_key = f"{dataset_title}/{basename(expected_staging_key)}"

    operation_name = any_operation_name()
    error_message = any_error_message()
    error = ClientError(
        ClientErrorResponseTypeDef(
            Error=ClientErrorResponseError(Code="NoSuchKey", Message=error_message)
        ),
        operation_name,
    )

    json_bytes = dumps(deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT)).encode()
    s3_client_response = StreamingBody(BytesIO(json_bytes), len(json_bytes))
    get_s3_client_for_role_mock.return_value.get_object.side_effect = [
        error,
        {S3_BODY_KEY: s3_client_response},
    ]

    expected_message = (
        f"'{expected_staging_key}' is not present in the staging bucket."
        f" Using '{expected_geostore_key}' from the geostore bucket for validation instead."
    )

    logger_mock = MagicMock()

    # When
    s3_url_reader = get_s3_url_reader(get_s3_role_arn(), dataset_title, logger_mock)
    s3_url_reader(s3_url)

    # Then
    with subtests.test(msg="log"):
        logger_mock.debug.assert_any_call(
            expected_message, extra={GIT_COMMIT: get_param(ParameterName.GIT_COMMIT)}
        )


@patch("geostore.s3_utils.get_s3_client_for_role")
def should_raise_any_client_error_other_than_no_such_key(
    get_s3_client_for_role_mock: MagicMock,
) -> None:
    # Given

    error_code = any_error_code()
    operation_name = any_operation_name()
    error_message = any_error_message()
    error = ClientError(
        ClientErrorResponseTypeDef(
            Error=ClientErrorResponseError(Code=error_code, Message=error_message)
        ),
        operation_name,
    )

    get_s3_client_for_role_mock.return_value.get_object.side_effect = error

    logger_mock = MagicMock()

    with raises(ClientError):
        s3_url_reader = get_s3_url_reader(get_s3_role_arn(), any_dataset_title(), logger_mock)
        s3_url_reader(any_s3_url())


def should_return_accurate_etag_value_for_empty_file() -> None:
    byte_representation_in_empty_file = b""
    expected_empty_file_etag = '"d41d8cd98f00b204e9800998ecf8427e"'

    etag_from_empty_file = calculate_s3_etag(byte_representation_in_empty_file)

    assert etag_from_empty_file == expected_empty_file_etag


def should_return_accurate_etag_value_for_single_chunk_size_file() -> None:
    byte_representation_in_small_file = b"01G9GZ9MX3GAYK1J28WMN4BRMG\n"
    expected_small_file_etag = '"2d0503d7761d6240b7a6bffca8ba25fa"'

    etag_from_small_file = calculate_s3_etag(byte_representation_in_small_file)

    assert etag_from_small_file == expected_small_file_etag


def should_return_accurate_etag_value_for_multi_chunk_size_file() -> None:
    byte_representation_in_big_file = b"01G9GZ89WNW43XYDB481MW27SH\n"
    expected_big_file_etag = '"a591c017ff7dc7944b9b9169953937fa-4"'

    # parse custom chunk_size to mimic big file behaviour in test
    etag_from_big_file = calculate_s3_etag(byte_representation_in_big_file, chunk_size=8)

    assert etag_from_big_file == expected_big_file_etag


@mark.infrastructure
def should_return_etag_if_s3_object_exists() -> None:
    key_prefix = any_safe_file_path()
    collection_metadata_filename = any_safe_filename()

    collection_dict = {
        **deepcopy(MINIMAL_VALID_STAC_COLLECTION_OBJECT),
    }

    with S3Object(
        file_object=json_dict_to_file_object(collection_dict),
        bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
        key=f"{key_prefix}/{collection_metadata_filename}",
    ):
        s3_etag = get_s3_etag(
            Resource.STORAGE_BUCKET_NAME.resource_name,
            f"{key_prefix}/{collection_metadata_filename}",
            get_log(),
        )

        # return mock etag string, generally 32 char or longer
        assert len(s3_etag) >= 32


@patch("geostore.s3_utils.get_s3_client_for_role")
def should_return_empty_string_if_s3_object_is_missing(
    get_s3_client_for_role_mock: MagicMock,
) -> None:
    # Given
    bucket = any_s3_bucket_name()
    collection_metadata_filename = any_safe_filename()

    operation_name = any_operation_name()
    error_message = any_error_message()
    error = ClientError(
        ClientErrorResponseTypeDef(
            Error=ClientErrorResponseError(Code="404", Message=error_message)
        ),
        operation_name,
    )

    get_s3_client_for_role_mock.return_value.head_object.side_effect = error

    logger_mock = MagicMock()

    etag = get_s3_etag(
        s3_bucket=bucket, s3_object_key=collection_metadata_filename, logger=logger_mock
    )
    assert etag == ""


@mark.infrastructure
def should_stop_s3stacio_from_put_object_if_locally_calculated_etag_is_identical_to_s3_etag(
    s3_client: S3Client,
) -> None:

    s3_stac_io = S3StacIO()
    bucket = Resource.STORAGE_BUCKET_NAME.resource_name
    collection_metadata_filename = any_safe_filename()

    s3_obj_url = f"s3://{bucket}/{collection_metadata_filename}"
    sample_obj_str = "01G9GZ9MX3GAYK1J28WMN4BRMG\n"

    try:
        # Write object to bucket - v1
        s3_stac_io.write_text(dest=s3_obj_url, txt=sample_obj_str)

        s3_response = s3_client.get_object(Bucket=bucket, Key=collection_metadata_filename)
        obj_version_id_one = s3_response["VersionId"]

        # Write same object to bucket again - v2
        s3_stac_io.write_text(dest=s3_obj_url, txt=sample_obj_str)
        s3_response = s3_client.get_object(Bucket=bucket, Key=collection_metadata_filename)
        obj_version_id_two = s3_response["VersionId"]

        # Ensure original object is not rewritten
        assert obj_version_id_one == obj_version_id_two

    finally:
        delete_s3_key(
            Resource.STORAGE_BUCKET_NAME.resource_name,
            collection_metadata_filename,
            s3_client,
        )


@patch("geostore.s3_utils.get_s3_client_for_role")
def should_log_error_message_if_failure_to_get_object_etag_is_other_than_no_such_key(
    get_s3_client_for_role_mock: MagicMock, subtests: SubTests
) -> None:
    # Given
    bucket = any_s3_bucket_name()
    collection_metadata_filename = any_safe_filename()

    error_code = any_error_code()
    operation_name = any_operation_name()
    error_message = any_error_message()
    error = ClientError(
        ClientErrorResponseTypeDef(
            Error=ClientErrorResponseError(Code=error_code, Message=error_message)
        ),
        operation_name,
    )

    get_s3_client_for_role_mock.return_value.head_object.side_effect = error

    expected_message = (
        f"Unable to fetch eTag for “{collection_metadata_filename}” "
        f"in s3://{bucket} due to “{error}”"
    )

    logger_mock = MagicMock()

    get_s3_etag(s3_bucket=bucket, s3_object_key=collection_metadata_filename, logger=logger_mock)

    # Then
    with subtests.test(msg="log"):
        logger_mock.debug.assert_any_call(
            expected_message, extra={GIT_COMMIT: get_param(ParameterName.GIT_COMMIT)}
        )
