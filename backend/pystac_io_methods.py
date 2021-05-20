from typing import IO, Union
from urllib.parse import urlparse

import boto3

S3_CLIENT = boto3.client("s3")


def read_method(uri: str) -> str:
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path[1:]
    obj = S3_CLIENT.get_object(Bucket=bucket, Key=key)
    result: str = obj["Body"].read().decode("utf-8")

    return result


def write_method(uri: str, body: Union[bytes, IO[bytes]]) -> None:
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path[1:]
    S3_CLIENT.put_object(Bucket=bucket, Key=key, Body=body)
