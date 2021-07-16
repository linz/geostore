from typing import IO, TYPE_CHECKING, Tuple, Union
from urllib.parse import urlparse

import boto3

from .boto3_config import CONFIG

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_s3 import S3Client
else:
    # In production we want to avoid depending on a package which has no runtime impact
    S3Client = object

S3_CLIENT: S3Client = boto3.client("s3", config=CONFIG)


def read_method(url: str) -> str:
    bucket, key = get_bucket_and_key_from_url(url)
    obj = S3_CLIENT.get_object(Bucket=bucket, Key=key)
    result: str = obj["Body"].read().decode("utf-8")

    return result


def write_method(url: str, body: Union[bytes, IO[bytes]]) -> None:
    bucket, key = get_bucket_and_key_from_url(url)
    S3_CLIENT.put_object(Bucket=bucket, Key=key, Body=body)


def get_bucket_and_key_from_url(url: str) -> Tuple[str, str]:
    parsed = urlparse(url)
    return parsed.netloc, parsed.path[1:]
