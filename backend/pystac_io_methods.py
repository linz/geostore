from typing import IO, Tuple, Union
from urllib.parse import urlparse

import boto3

S3_CLIENT = boto3.client("s3")


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
