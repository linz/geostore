from typing import TYPE_CHECKING

import boto3
import smart_open

from ..boto3_config import CONFIG
from ..import_dataset_file import get_import_result
from ..s3 import CHUNK_SIZE, S3_URL_PREFIX
from ..types import JsonObject

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_s3 import S3Client
else:
    # In production we want to avoid depending on a package which has no runtime impact
    S3Client = object  # pragma: no mutate

TARGET_S3_CLIENT: S3Client = boto3.client("s3", config=CONFIG)


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    return get_import_result(event, importer)


def importer(
    source_bucket_name: str,
    original_key: str,
    target_bucket_name: str,
    new_key: str,
    source_s3_client: S3Client,
) -> None:
    source_response = source_s3_client.get_object(Bucket=source_bucket_name, Key=original_key)

    # TODO: Simplify once boto3 issue #426 is actually fixed pylint:disable=fixme
    with smart_open.open(
        f"{S3_URL_PREFIX}{target_bucket_name}/{new_key}",
        mode="wb",
        transport_params={"client": TARGET_S3_CLIENT},
    ) as target_file:
        for chunk in source_response["Body"].iter_chunks(chunk_size=CHUNK_SIZE):
            target_file.write(chunk)
