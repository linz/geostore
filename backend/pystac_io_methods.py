from typing import IO, Union
from urllib.parse import urlparse

import boto3
from pystac import STAC_IO  # type: ignore[import]

from .s3 import S3_SCHEMA

S3_CLIENT = boto3.client("s3")


def read_method(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme == S3_SCHEMA:
        bucket = parsed.netloc
        key = parsed.path[1:]
        obj = S3_CLIENT.get_object(Bucket=bucket, Key=key)
        result: str = obj["Body"].read().decode("utf-8")
    else:
        result = STAC_IO.default_read_text_method(uri)

    return result


def write_method(uri: str, body: Union[bytes, IO[bytes]]) -> None:
    parsed = urlparse(uri)
    if parsed.scheme == S3_SCHEMA:
        bucket = parsed.netloc
        key = parsed.path[1:]
        S3_CLIENT.put_object(Bucket=bucket, Key=key, Body=body)
    else:
        STAC_IO.default_write_text_method(uri, body)
