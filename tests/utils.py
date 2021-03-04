import string
from contextlib import AbstractContextManager
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from io import StringIO
from json import dump
from os import urandom
from random import choice, getrandbits, randrange
from types import TracebackType
from typing import Any, BinaryIO, Dict, List, Optional, TextIO, Type
from unittest.mock import Mock
from uuid import uuid4

import boto3
from botocore.auth import EMPTY_SHA256_HASH  # type: ignore[import]
from multihash import SHA2_256  # type: ignore[import]
from mypy_boto3_s3.type_defs import DeleteTypeDef, ObjectIdentifierTypeDef

from backend.content_iterator.task import MAX_ITERATION_SIZE
from backend.model import DatasetModel
from backend.utils import DATASET_TYPES

REFERENCE_DATETIME = datetime(2000, 1, 1, tzinfo=timezone.utc)
DELETE_OBJECTS_MAX_KEYS = 1000

STAC_VERSION = "1.0.0-rc.1"

SHA256_BYTE_COUNT = len(EMPTY_SHA256_HASH) >> 1
EMPTY_FILE_MULTIHASH = f"{SHA2_256:x}{SHA256_BYTE_COUNT:x}{EMPTY_SHA256_HASH}"

# General-purpose generators


def random_string(length: int) -> str:
    """
    Includes ASCII printable characters and the first printable character from several Unicode
    blocks <https://en.wikipedia.org/wiki/List_of_Unicode_characters>.
    """
    return _random_string_choices(f"{string.printable}¡ĀƀḂəʰͰἀЀ–⁰₠℀⅐←∀⌀①─▀■☀🬀✁ㄅﬀ", length)


def random_ascii_letter_string(length: int) -> str:
    return _random_string_choices(string.ascii_letters, length)


def _random_string_choices(characters: str, length: int) -> str:
    return "".join(choice(characters) for _ in range(length))


def any_past_datetime() -> datetime:
    return REFERENCE_DATETIME - timedelta(seconds=randrange(60_000_000_000))  # Back to year 98


def any_past_datetime_string() -> str:
    return any_past_datetime().isoformat()


def any_program_name() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_safe_file_path() -> str:
    paths = [any_safe_filename() for _ in range(randrange(1, 5))]
    return "/".join(paths)


def any_safe_filename() -> str:
    return _random_string_choices(f"{string.digits}{string.ascii_letters}", 20)


def any_host() -> str:
    return random_ascii_letter_string(20)


def any_https_url() -> str:
    host = any_host()
    path = any_safe_file_path()
    return f"https://{host}/{path}"


def any_hex_multihash() -> str:
    hex_digest = any_sha256_hex_digest()
    return sha256_hex_digest_to_multihash(hex_digest)


def any_sha256_hex_digest() -> str:
    return sha256(random_string(20).encode()).hexdigest()


def sha256_hex_digest_to_multihash(hex_digest: str) -> str:
    return f"{SHA2_256:x}{32:x}{hex_digest}"


def any_file_contents() -> bytes:
    """Arbitrary-length bytes"""
    return urandom(20)


def any_error_message() -> str:
    """Arbitrary-length string"""
    return random_string(50)


def any_dictionary_key() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_boolean() -> bool:
    return bool(getrandbits(1))


# STAC-specific generators


def any_dataset_id() -> str:
    return uuid4().hex


def any_dataset_version_id() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_valid_dataset_type() -> str:
    return choice(DATASET_TYPES)


def any_dataset_title() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_dataset_description() -> str:
    """Arbitrary-length string"""
    return random_string(100)


def any_dataset_owning_group() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_stac_relation() -> str:
    return choice(["child", "root", "self"])


def any_stac_asset_name() -> str:
    """Arbitrary-length string"""
    return random_string(20)


# AWS generators


def any_s3_url() -> str:
    bucket_name = any_s3_bucket_name()
    key = any_safe_file_path()
    return f"s3://{bucket_name}/{key}"


def any_s3_bucket_name() -> str:
    return _random_string_choices(f"{string.digits}{string.ascii_lowercase}", 20)


def any_lambda_context() -> bytes:
    """Arbitrary-length string"""
    return random_string(10).encode()


def any_item_index() -> int:
    """Arbitrary non-negative multiple of iteration size"""
    return randrange(1_000_000) * MAX_ITERATION_SIZE


def any_batch_job_array_index() -> int:
    # https://docs.aws.amazon.com/batch/latest/userguide/array_jobs.html
    return randrange(2, 10_001)


def any_item_count() -> int:
    """Arbitrary non-negative integer"""
    return randrange(3)


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
        version_list = self._get_object_versions()
        self._delete_object_versions(version_list)

    def _delete_object_versions(self, version_list: List[ObjectIdentifierTypeDef]) -> None:
        for index in range(0, len(version_list), DELETE_OBJECTS_MAX_KEYS):
            response = self._s3_client.delete_objects(
                Bucket=self.bucket_name,
                Delete=DeleteTypeDef(Objects=version_list[index : index + DELETE_OBJECTS_MAX_KEYS]),
            )
            print(response)

    def _get_object_versions(self) -> List[ObjectIdentifierTypeDef]:
        version_list: List[ObjectIdentifierTypeDef] = []
        object_versions_paginator = self._s3_client.get_paginator("list_object_versions")
        for object_versions_page in object_versions_paginator.paginate(Bucket=self.bucket_name):
            for marker in object_versions_page.get("DeleteMarkers", []):
                if marker["Key"] == self.key:
                    version_list.append({"Key": self.key, "VersionId": marker["VersionId"]})
            for version in object_versions_page.get("Versions", []):
                if version["Key"] == self.key:
                    version_list.append({"Key": self.key, "VersionId": version["VersionId"]})
        return version_list


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
