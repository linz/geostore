from typing import TYPE_CHECKING

import boto3

from ..import_dataset_file import get_import_result
from ..types import JsonObject

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import CopyObjectOutputTypeDef
else:
    # In production we want to avoid depending on a package which has no runtime impact
    CopyObjectOutputTypeDef = JsonObject
    S3Client = object

S3_CLIENT: S3Client = boto3.client("s3")


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
