from typing import TYPE_CHECKING

import boto3

from ..import_dataset_file import get_import_result
from ..types import JsonObject

S3_CLIENT = boto3.client("s3")

if TYPE_CHECKING:
    from mypy_boto3_s3.type_defs import CopyObjectOutputTypeDef
else:
    CopyObjectOutputTypeDef = JsonObject


def lambda_handler(event: JsonObject, _context: bytes) -> JsonObject:
    return get_import_result(event, importer)


def importer(
    source_bucket_name: str, original_key: str, target_bucket_name: str, new_key: str
) -> CopyObjectOutputTypeDef:
    return S3_CLIENT.copy_object(
        CopySource={"Bucket": source_bucket_name, "Key": original_key},
        Bucket=target_bucket_name,
        Key=new_key,
    )
