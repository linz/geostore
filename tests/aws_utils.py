import string
import time
from contextlib import AbstractContextManager
from datetime import datetime, timedelta
from io import StringIO
from json import dump
from random import choice, randrange
from types import TracebackType
from typing import Any, BinaryIO, Dict, List, Optional, TextIO, Tuple, Type, get_args
from unittest.mock import Mock
from uuid import uuid4

import boto3
from botocore.auth import EMPTY_SHA256_HASH  # type: ignore[import]
from multihash import SHA2_256  # type: ignore[import]
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import DeleteTypeDef, ObjectIdentifierTypeDef
from mypy_boto3_s3control import S3ControlClient
from mypy_boto3_s3control.literals import JobStatusType
from mypy_boto3_s3control.type_defs import DescribeJobResultTypeDef
from pytest_subtests import SubTests  # type: ignore[import]

from backend.content_iterator.task import MAX_ITERATION_SIZE
from backend.datasets_model import DatasetsModelBase, datasets_model_with_meta
from backend.import_file_batch_job_id_keys import ASSET_JOB_ID_KEY, METADATA_JOB_ID_KEY
from backend.models import CHECK_ID_PREFIX, DATASET_ID_PREFIX, DB_KEY_SEPARATOR, URL_ID_PREFIX
from backend.parameter_store import ParameterName, get_param
from backend.populate_catalog.task import CONTENTS_KEY
from backend.processing_assets_model import (
    ProcessingAssetsModelBase,
    processing_assets_model_with_meta,
)
from backend.s3 import S3_URL_PREFIX
from backend.types import JsonObject
from backend.validation_results_model import (
    ValidationResult,
    ValidationResultsModelBase,
    validation_results_model_with_meta,
)

from .general_generators import (
    _random_string_choices,
    any_past_datetime,
    any_safe_file_path,
    random_string,
)
from .stac_generators import any_dataset_id, any_dataset_title

SHA256_BYTE_COUNT = len(EMPTY_SHA256_HASH) >> 1
EMPTY_FILE_MULTIHASH = f"{SHA2_256:x}{SHA256_BYTE_COUNT:x}{EMPTY_SHA256_HASH}"

DELETE_OBJECTS_MAX_KEYS = 1000

S3_BATCH_JOB_COMPLETED_STATE = "Complete"
S3_BATCH_JOB_FINAL_STATES = [S3_BATCH_JOB_COMPLETED_STATE, "Failed", "Cancelled"]


def any_arn_formatted_string() -> str:
    return f"arn:aws:states:{random_string(5)}:{string.digits}:execution:yy:xx"


# Batch


def any_batch_job_array_index() -> int:
    # https://docs.aws.amazon.com/batch/latest/userguide/array_jobs.html
    return randrange(2, 10_001)


def any_next_item_index() -> int:
    """Arbitrary non-negative multiple of iteration size"""
    return randrange(1_000_000) * MAX_ITERATION_SIZE


def any_batch_job_status() -> str:
    job_status: str = choice(get_args(JobStatusType))
    return job_status


# DynamoDB


def any_item_count() -> int:
    """Arbitrary non-negative integer"""
    return randrange(3)


def any_table_name() -> str:
    return random_string(15)


# IAM


def any_account_id() -> int:
    return randrange(1_000_000_000_000)


# Lambda


def any_lambda_context() -> bytes:
    """Arbitrary-length string"""
    return random_string(10).encode()


# S3


def any_s3_url() -> str:
    bucket_name = any_s3_bucket_name()
    key = any_safe_file_path()
    return f"{S3_URL_PREFIX}{bucket_name}/{key}"


def any_s3_bucket_name() -> str:
    return _random_string_choices(f"{string.digits}{string.ascii_lowercase}", 20)


def any_s3_bucket_arn() -> str:
    return f"arn:aws:s3:::{any_s3_bucket_name()}"


def any_job_id() -> str:
    return uuid4().hex


def any_invocation_schema_version() -> str:
    """Arbitrary-length string"""
    return random_string(10)


def any_invocation_id() -> str:
    """Arbitrary-length string"""
    return random_string(10)


def any_task_id() -> str:
    """Arbitrary-length string"""
    return random_string(10)


def any_operation_name() -> str:
    """Arbitrary-length string"""
    return random_string(10)


# Context managers


class Dataset:
    def __init__(self, title: Optional[str] = None):
        if title is None:
            title = any_dataset_title()

        dataset_id = any_dataset_id()

        datasets_model_class = datasets_model_with_meta()
        self._item = datasets_model_class(
            id=f"{DATASET_ID_PREFIX}{dataset_id}",
            title=title,
            created_at=any_past_datetime(),
            updated_at=any_past_datetime(),
        )

    def __enter__(self) -> DatasetsModelBase:
        self._item.save()
        return self._item

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self._item.delete()


class ProcessingAsset:
    index = 0

    def __init__(
        self,
        asset_id: str,
        url: str,
        multihash: Optional[str] = None,
    ):
        prefix = "METADATA" if multihash is None else "DATA"

        processing_assets_model = processing_assets_model_with_meta()
        self._item = processing_assets_model(
            hash_key=asset_id,
            range_key=f"{prefix}_ITEM_INDEX{DB_KEY_SEPARATOR}{self.index}",
            url=url,
            multihash=multihash,
        )
        ProcessingAsset.index += 1

    def __enter__(self) -> ProcessingAssetsModelBase:
        self._item.save()
        return self._item

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self._item.delete()


class ValidationItem:
    def __init__(  # pylint: disable=too-many-arguments
        self,
        asset_id: str,
        result: ValidationResult,
        details: JsonObject,
        url: str,
        check: str,
    ):
        validation_results_model = validation_results_model_with_meta(
            get_param(ParameterName.STORAGE_VALIDATION_RESULTS_TABLE_NAME)
        )
        self._item = validation_results_model(
            pk=asset_id,
            sk=f"{CHECK_ID_PREFIX}{check}{DB_KEY_SEPARATOR}{URL_ID_PREFIX}{url}",
            result=result.value,
            details=details,
        )

    def __enter__(self) -> ValidationResultsModelBase:
        self._item.save()
        return self._item

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self._item.delete()


class S3Object(AbstractContextManager):  # type: ignore[type-arg]
    def __init__(self, /, file_object: BinaryIO, bucket_name: str, key: str):
        super().__init__()

        self.file_object = file_object
        self.bucket_name = bucket_name
        self.key = key
        self.url = f"{S3_URL_PREFIX}{self.bucket_name}/{self.key}"
        self._s3_client: S3Client = boto3.client("s3")

    def __enter__(self) -> "S3Object":
        self._s3_client.upload_fileobj(self.file_object, self.bucket_name, self.key)
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        delete_s3_key(self.bucket_name, self.key, self._s3_client)


# Special-purpose mocks


class MockJSONURLReader(Mock):
    def __init__(
        self, url_to_json: Dict[str, Any], call_limit: Optional[int] = None, **kwargs: Any
    ):
        super().__init__(**kwargs)

        self.url_to_json = url_to_json
        self.call_limit = call_limit
        self.side_effect = self.read_url

    def read_url(self, url: str) -> TextIO:
        if self.call_limit is not None:
            assert self.call_count <= self.call_limit

        json_dict_or_io = self.url_to_json[url]
        if isinstance(json_dict_or_io, StringIO):
            json_dict_or_io.seek(0)
            return json_dict_or_io

        result = StringIO()
        dump(json_dict_or_io, result)
        result.seek(0)
        return result


class MockValidationResultFactory(Mock):
    pass


# Utility functions


def wait_for_s3_key(bucket_name: str, key: str, s3_client: S3Client) -> None:

    process_timeout = datetime.now() + timedelta(minutes=3)
    while CONTENTS_KEY not in s3_client.list_objects(Bucket=bucket_name, Prefix=key):
        assert (  # pragma: no cover
            datetime.now() < process_timeout
        ), f"S3 file '{bucket_name}/{key}' was not created, process timed out."
        time.sleep(5)  # pragma: no cover


def delete_s3_key(bucket_name: str, key: str, s3_client: S3Client) -> None:
    version_list = get_s3_key_versions(bucket_name, key, s3_client)
    delete_s3_versions(bucket_name, version_list, s3_client)


def delete_s3_prefix(bucket_name: str, prefix: str, s3_client: S3Client) -> None:
    version_list = get_s3_prefix_versions(bucket_name, prefix, s3_client)
    delete_s3_versions(bucket_name, version_list, s3_client)


def delete_s3_versions(
    bucket_name: str, version_list: List[ObjectIdentifierTypeDef], s3_client: S3Client
) -> None:
    for index in range(0, len(version_list), DELETE_OBJECTS_MAX_KEYS):
        response = s3_client.delete_objects(
            Bucket=bucket_name,
            Delete=DeleteTypeDef(Objects=version_list[index : index + DELETE_OBJECTS_MAX_KEYS]),
        )
        print(response)


def get_s3_key_versions(
    bucket_name: str, key: str, s3_client: S3Client
) -> List[ObjectIdentifierTypeDef]:
    version_list: List[ObjectIdentifierTypeDef] = []
    object_versions_paginator = s3_client.get_paginator("list_object_versions")
    for object_versions_page in object_versions_paginator.paginate(Bucket=bucket_name, Prefix=key):
        for version in object_versions_page.get("Versions", []):
            if version["Key"] == key:
                version_list.append({"Key": version["Key"], "VersionId": version["VersionId"]})
    assert version_list, version_list
    return version_list


def get_s3_prefix_versions(
    bucket_name: str, prefix: str, s3_client: S3Client
) -> List[ObjectIdentifierTypeDef]:
    version_list: List[ObjectIdentifierTypeDef] = []
    object_versions_paginator = s3_client.get_paginator("list_object_versions")
    for object_versions_page in object_versions_paginator.paginate(
        Bucket=bucket_name, Prefix=prefix
    ):
        for version in object_versions_page.get("Versions", []):
            version_list.append({"Key": version["Key"], "VersionId": version["VersionId"]})
    assert version_list, version_list
    return version_list


def s3_object_arn_to_key(arn: str) -> str:
    bucket_and_key = arn.split(sep=":", maxsplit=5)[-1]
    return bucket_and_key.split(sep="/", maxsplit=1)[-1]


def wait_for_copy_jobs(
    import_dataset_response: JsonObject,
    account_id: str,
    s3_control_client: S3ControlClient,
    subtests: SubTests,
) -> Tuple[DescribeJobResultTypeDef, DescribeJobResultTypeDef]:
    with subtests.test(msg="Should complete metadata copy operation successfully"):
        metadata_copy_job_result = wait_for_s3_batch_job_completion(
            import_dataset_response[METADATA_JOB_ID_KEY], account_id, s3_control_client
        )

    with subtests.test(msg="Should complete asset copy operation successfully"):
        asset_copy_job_result = wait_for_s3_batch_job_completion(
            import_dataset_response[ASSET_JOB_ID_KEY], account_id, s3_control_client
        )

    return metadata_copy_job_result, asset_copy_job_result


def wait_for_s3_batch_job_completion(
    s3_batch_job_arn: str,
    account_id: str,
    s3_control_client: S3ControlClient,
) -> DescribeJobResultTypeDef:
    process_timeout = datetime.now() + timedelta(minutes=3)

    while (
        job_result := s3_control_client.describe_job(
            AccountId=account_id,
            JobId=s3_batch_job_arn,
        )
    )["Job"]["Status"] not in S3_BATCH_JOB_FINAL_STATES:

        assert (  # pragma: no cover
            datetime.now() < process_timeout
        ), f"S3 Batch process {job_result['Job']['JobId']} hasn't completed, process timed out."

        time.sleep(5)  # pragma: no cover

    assert job_result["Job"]["Status"] == S3_BATCH_JOB_COMPLETED_STATE, job_result

    return job_result


def delete_copy_job_files(
    metadata_copy_job_result: DescribeJobResultTypeDef,
    asset_copy_job_result: DescribeJobResultTypeDef,
    storage_bucket_name: str,
    s3_client: S3Client,
    subtests: SubTests,
) -> None:
    for response in [metadata_copy_job_result, asset_copy_job_result]:
        manifest_key = s3_object_arn_to_key(response["Job"]["Manifest"]["Location"]["ObjectArn"])
        with subtests.test(msg=f"Delete {manifest_key}"):
            delete_s3_key(storage_bucket_name, manifest_key, s3_client)

    copy_job_report_prefix = asset_copy_job_result["Job"]["Report"]["Prefix"]
    with subtests.test(msg=f"Delete {copy_job_report_prefix}"):
        delete_s3_prefix(storage_bucket_name, copy_job_report_prefix, s3_client)
