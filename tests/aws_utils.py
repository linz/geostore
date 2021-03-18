import string
from contextlib import AbstractContextManager
from io import StringIO
from json import dump
from random import randrange
from types import TracebackType
from typing import Any, BinaryIO, Dict, List, Optional, TextIO, Type
from unittest.mock import Mock
from uuid import uuid4

import boto3
from botocore.auth import EMPTY_SHA256_HASH  # type: ignore[import]
from multihash import SHA2_256  # type: ignore[import]
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import DeleteTypeDef, ObjectIdentifierTypeDef

from backend.content_iterator.task import MAX_ITERATION_SIZE
from backend.dataset_model import DatasetModel
from backend.processing_assets_model import ProcessingAssetsModel
from backend.types import JsonObject
from backend.validation_results_model import ValidationResult, ValidationResultsModel

from .general_generators import (
    _random_string_choices,
    any_past_datetime,
    any_past_datetime_string,
    any_safe_file_path,
    random_string,
)
from .stac_generators import (
    any_dataset_description,
    any_dataset_id,
    any_dataset_owning_group,
    any_dataset_title,
    any_valid_dataset_type,
)

STAC_VERSION = "1.0.0-rc.1"

SHA256_BYTE_COUNT = len(EMPTY_SHA256_HASH) >> 1
EMPTY_FILE_MULTIHASH = f"{SHA2_256:x}{SHA256_BYTE_COUNT:x}{EMPTY_SHA256_HASH}"

MINIMAL_VALID_STAC_OBJECT: Dict[str, Any] = {
    "description": any_dataset_description(),
    "extent": {
        "spatial": {"bbox": [[-180, -90, 180, 90]]},
        "temporal": {"interval": [[any_past_datetime_string(), None]]},
    },
    "id": any_dataset_id(),
    "license": "MIT",
    "links": [],
    "stac_version": STAC_VERSION,
    "type": "Collection",
}

DELETE_OBJECTS_MAX_KEYS = 1000


def any_arn_formatted_string() -> str:
    return f"arn:aws:states:{random_string(5)}:{string.digits}:execution:yy:xx"


# Batch


def any_batch_job_array_index() -> int:
    # https://docs.aws.amazon.com/batch/latest/userguide/array_jobs.html
    return randrange(2, 10_001)


def any_item_index() -> int:
    """Arbitrary non-negative multiple of iteration size"""
    return randrange(1_000_000) * MAX_ITERATION_SIZE


# DynamoDB


def any_item_count() -> int:
    """Arbitrary non-negative integer"""
    return randrange(3)


# Lambda


def any_lambda_context() -> bytes:
    """Arbitrary-length string"""
    return random_string(10).encode()


# S3


def any_s3_url() -> str:
    bucket_name = any_s3_bucket_name()
    key = any_safe_file_path()
    return f"s3://{bucket_name}/{key}"


def any_s3_bucket_name() -> str:
    return _random_string_choices(f"{string.digits}{string.ascii_lowercase}", 20)


def any_job_id() -> str:
    return uuid4().hex


# Context managers


class Dataset:
    def __init__(
        self,
        dataset_id: Optional[str] = None,
        dataset_type: Optional[str] = None,
        title: Optional[str] = None,
        owning_group: Optional[str] = None,
    ):
        if dataset_id is None:
            dataset_id = any_dataset_id()

        if dataset_type is None:
            dataset_type = any_valid_dataset_type()

        if title is None:
            title = any_dataset_title()

        if owning_group is None:
            owning_group = any_dataset_owning_group()

        self._item = DatasetModel(
            id=f"DATASET#{dataset_id}",
            type=f"TYPE#{dataset_type}",
            title=title,
            owning_group=owning_group,
            created_at=any_past_datetime(),
            updated_at=any_past_datetime(),
        )

    def __enter__(self) -> DatasetModel:
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
    def __init__(
        self,
        asset_id: str,
        url: str,
        multihash: Optional[str] = None,
    ):
        prefix = "METADATA" if multihash is None else "DATA"

        self._item = ProcessingAssetsModel(
            hash_key=asset_id,
            range_key=f"{prefix}_ITEM_INDEX#0",
            url=url,
            multihash=multihash,
        )

    def __enter__(self) -> ProcessingAssetsModel:
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

        self._item = ValidationResultsModel(
            pk=asset_id,
            sk=f"CHECK#{check}#URL#{url}",
            result=result.value,
            details=details,
        )

    def __enter__(self) -> ValidationResultsModel:
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
        self.url = f"s3://{self.bucket_name}/{self.key}"
        self._s3_client = boto3.client("s3")

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

        result = StringIO()
        dump(self.url_to_json[url], result)
        result.seek(0)
        return result


class MockValidationResultFactory(Mock):
    pass


# Utility functions


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
        for marker in object_versions_page.get("DeleteMarkers", []):
            if marker["Key"] == key:
                version_list.append({"Key": marker["Key"], "VersionId": marker["VersionId"]})
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
        for marker in object_versions_page.get("DeleteMarkers", []):
            version_list.append({"Key": marker["Key"], "VersionId": marker["VersionId"]})
        for version in object_versions_page.get("Versions", []):
            version_list.append({"Key": version["Key"], "VersionId": version["VersionId"]})
    assert version_list, version_list
    return version_list


def s3_object_arn_to_key(arn: str) -> str:
    bucket_and_key = arn.split(sep=":", maxsplit=5)[-1]
    return bucket_and_key.split(sep="/", maxsplit=1)[-1]
