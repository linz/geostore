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

from geostore.import_asset_file.task import importer
from geostore.import_metadata_file.task import S3_BODY_KEY
from geostore.logging_keys import GIT_COMMIT
from geostore.parameter_store import ParameterName, get_param
from geostore.populate_catalog.task import CATALOG_FILENAME
from geostore.pystac_io_methods import S3StacIO
from geostore.resources import Resource
from geostore.s3 import S3_URL_PREFIX, get_s3_client_for_role
from geostore.s3_utils import (
    KNOWN_ETAG_OF_EMPTY_FILE,
    calculate_s3_etag,
    get_s3_etag,
    get_s3_url_reader,
)
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
from tests.general_generators import (
    any_error_message,
    any_file_contents,
    any_safe_file_path,
    any_safe_filename,
)
from tests.stac_generators import any_dataset_title
from tests.stac_objects import MINIMAL_VALID_STAC_COLLECTION_OBJECT

if TYPE_CHECKING:
    from botocore.exceptions import _ClientErrorResponseError, _ClientErrorResponseTypeDef
else:
    _ClientErrorResponseError = _ClientErrorResponseTypeDef = dict

# https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html
# single chunk etag is 32 characters in length, (+2) with quotes as they are part of etag
SINGLE_CHUNK_ETAG_LENGTH = 34
# multi chunk etag includes a -x suffix (+2 characters), where x specifies the number of chunks
MINIMUM_MULTI_CHUNK_ETAG_LENGTH = 36  # (+1) for every 10th multiplier

# https://awscli.amazonaws.com/v2/documentation/api/latest/topic/s3-config.html#multipart-chunksize
S3_DEFAULT_CHUNK_SIZE = 8_388_608


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
        _ClientErrorResponseTypeDef(
            Error=_ClientErrorResponseError(Code="NoSuchKey", Message=error_message)
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
        _ClientErrorResponseTypeDef(
            Error=_ClientErrorResponseError(Code=error_code, Message=error_message)
        ),
        operation_name,
    )

    get_s3_client_for_role_mock.return_value.get_object.side_effect = error

    logger_mock = MagicMock()

    with raises(ClientError):
        s3_url_reader = get_s3_url_reader(get_s3_role_arn(), any_dataset_title(), logger_mock)
        s3_url_reader(any_s3_url())


@mark.infrastructure
def should_return_accurate_etag_value_for_empty_file(s3_client: S3Client) -> None:
    asset_filename = any_safe_filename()
    asset_contents = any_file_contents(byte_count=0)

    staging_bucket = Resource.STAGING_BUCKET_NAME.resource_name
    storage_bucket = Resource.STORAGE_BUCKET_NAME.resource_name

    local_etag = calculate_s3_etag(asset_contents)

    with S3Object(
        file_object=BytesIO(initial_bytes=asset_contents),
        bucket_name=staging_bucket,
        key=asset_filename,
    ):
        try:
            # import from staging to storage to ensure aws hasn't changed how etag is calculated
            importer(
                source_bucket_name=staging_bucket,
                original_key=asset_filename,
                target_bucket_name=storage_bucket,
                new_key=asset_filename,
                source_s3_client=get_s3_client_for_role(get_s3_role_arn()),
            )

            s3_etag = get_s3_etag(
                s3_bucket=storage_bucket, s3_object_key=f"{asset_filename}", logger=get_log()
            )

            assert local_etag == KNOWN_ETAG_OF_EMPTY_FILE
            assert local_etag == s3_etag
        finally:
            delete_s3_key(Resource.STORAGE_BUCKET_NAME.resource_name, asset_filename, s3_client)


@mark.infrastructure
def should_return_accurate_etag_value_for_file_smaller_than_single_chunk_size(
    s3_client: S3Client,
) -> None:
    asset_filename = any_safe_filename()
    asset_contents = any_file_contents(byte_count=S3_DEFAULT_CHUNK_SIZE - 1)

    staging_bucket = Resource.STAGING_BUCKET_NAME.resource_name
    storage_bucket = Resource.STORAGE_BUCKET_NAME.resource_name

    local_etag = calculate_s3_etag(asset_contents)

    with S3Object(
        file_object=BytesIO(initial_bytes=asset_contents),
        bucket_name=staging_bucket,
        key=f"{asset_filename}",
    ):
        try:
            # import from staging to storage to ensure aws hasn't changed how etag is calculated
            importer(
                source_bucket_name=staging_bucket,
                original_key=asset_filename,
                target_bucket_name=storage_bucket,
                new_key=asset_filename,
                source_s3_client=get_s3_client_for_role(get_s3_role_arn()),
            )

            s3_etag = get_s3_etag(
                s3_bucket=storage_bucket, s3_object_key=f"{asset_filename}", logger=get_log()
            )

            # ensure single chunk size file
            assert len(local_etag) == SINGLE_CHUNK_ETAG_LENGTH
            assert local_etag == s3_etag
        finally:
            delete_s3_key(Resource.STORAGE_BUCKET_NAME.resource_name, asset_filename, s3_client)


@mark.infrastructure
def should_return_accurate_etag_value_for_single_chunk_size_file(
    s3_client: S3Client,
) -> None:
    asset_filename = any_safe_filename()
    asset_contents = any_file_contents(byte_count=S3_DEFAULT_CHUNK_SIZE)

    staging_bucket = Resource.STAGING_BUCKET_NAME.resource_name
    storage_bucket = Resource.STORAGE_BUCKET_NAME.resource_name

    local_etag = calculate_s3_etag(asset_contents)

    with S3Object(
        file_object=BytesIO(initial_bytes=asset_contents),
        bucket_name=staging_bucket,
        key=f"{asset_filename}",
    ):
        try:
            # import from staging to storage to ensure aws hasn't changed how etag is calculated
            importer(
                source_bucket_name=staging_bucket,
                original_key=asset_filename,
                target_bucket_name=storage_bucket,
                new_key=asset_filename,
                source_s3_client=get_s3_client_for_role(get_s3_role_arn()),
            )

            s3_etag = get_s3_etag(
                s3_bucket=storage_bucket, s3_object_key=f"{asset_filename}", logger=get_log()
            )

            # file size exactly at single chunk returns 34 characters etag rather than 32 characters
            # (i.e. "656dadd6d61e0ebfd29264e34d742df3-1") where -1 suffix denotes 1 chunk
            assert len(local_etag) == SINGLE_CHUNK_ETAG_LENGTH + 2  # +2 characters for -1 suffix
            assert local_etag == s3_etag
        finally:
            delete_s3_key(Resource.STORAGE_BUCKET_NAME.resource_name, asset_filename, s3_client)


@mark.infrastructure
def should_return_accurate_etag_value_for_multi_chunk_size_file(s3_client: S3Client) -> None:
    asset_filename = any_safe_filename()
    asset_contents = any_file_contents(byte_count=S3_DEFAULT_CHUNK_SIZE + 1)

    staging_bucket = Resource.STAGING_BUCKET_NAME.resource_name
    storage_bucket = Resource.STORAGE_BUCKET_NAME.resource_name

    local_etag = calculate_s3_etag(asset_contents)

    with S3Object(
        file_object=BytesIO(initial_bytes=asset_contents),
        bucket_name=staging_bucket,
        key=asset_filename,
    ):
        try:
            # import from staging to storage to ensure aws hasn't changed how etag is calculated
            importer(
                source_bucket_name=staging_bucket,
                original_key=asset_filename,
                target_bucket_name=storage_bucket,
                new_key=asset_filename,
                source_s3_client=get_s3_client_for_role(get_s3_role_arn()),
            )

            s3_etag = get_s3_etag(
                s3_bucket=storage_bucket, s3_object_key=f"{asset_filename}", logger=get_log()
            )

            # ensure multi chunk size file
            assert len(local_etag) == MINIMUM_MULTI_CHUNK_ETAG_LENGTH
            assert local_etag == s3_etag
        finally:
            delete_s3_key(Resource.STORAGE_BUCKET_NAME.resource_name, asset_filename, s3_client)


@patch("geostore.s3_utils.get_s3_client_for_role")
def should_return_none_etag_if_s3_object_is_missing(
    get_s3_client_for_role_mock: MagicMock,
) -> None:
    # Given
    bucket = any_s3_bucket_name()
    collection_metadata_filename = any_safe_filename()

    operation_name = any_operation_name()
    error_message = any_error_message()
    error = ClientError(
        _ClientErrorResponseTypeDef(
            Error=_ClientErrorResponseError(Code="404", Message=error_message)
        ),
        operation_name,
    )

    get_s3_client_for_role_mock.return_value.head_object.side_effect = error

    logger_mock = MagicMock()

    etag = get_s3_etag(
        s3_bucket=bucket, s3_object_key=collection_metadata_filename, logger=logger_mock
    )
    assert etag is None


@mark.infrastructure
def should_not_overwrite_s3_file_when_etag_is_unchanged(
    s3_client: S3Client,
) -> None:
    s3_stac_io = S3StacIO()
    bucket = Resource.STORAGE_BUCKET_NAME.resource_name
    collection_metadata_filename = any_safe_filename()

    s3_obj_url = f"s3://{bucket}/{collection_metadata_filename}"
    asset_contents = any_file_contents()

    try:
        # Write object to bucket - first attempt
        s3_stac_io.write_text(dest=s3_obj_url, txt=str(asset_contents))

        s3_response = s3_client.get_object(Bucket=bucket, Key=collection_metadata_filename)
        version_id_after_first_write_attempt = s3_response["VersionId"]

        # Write same object to bucket again - second attempt
        s3_stac_io.write_text(dest=s3_obj_url, txt=str(asset_contents))
        s3_response = s3_client.get_object(Bucket=bucket, Key=collection_metadata_filename)
        version_id_after_second_write_attempt = s3_response["VersionId"]

        # Ensure original object is not rewritten
        assert version_id_after_first_write_attempt == version_id_after_second_write_attempt

    finally:
        delete_s3_key(
            Resource.STORAGE_BUCKET_NAME.resource_name,
            collection_metadata_filename,
            s3_client,
        )


@patch("geostore.s3_utils.get_s3_client_for_role")
def should_log_error_message_if_failure_to_get_object_etag_is_other_than_no_such_key(
    get_s3_client_for_role_mock: MagicMock,
) -> None:
    # Given
    bucket = any_s3_bucket_name()
    collection_metadata_filename = any_safe_filename()

    error_code = any_error_code()
    operation_name = any_operation_name()
    error_message = any_error_message()
    error = ClientError(
        _ClientErrorResponseTypeDef(
            Error=_ClientErrorResponseError(Code=error_code, Message=error_message)
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
    logger_mock.debug.assert_any_call(
        expected_message, extra={GIT_COMMIT: get_param(ParameterName.GIT_COMMIT)}
    )
