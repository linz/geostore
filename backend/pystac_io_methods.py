from typing import IO, Any, Union
from urllib.parse import urlparse

import boto3
from pystac import STAC_IO  # type: ignore[import]

S3_CLIENT = boto3.client("s3")


def read_method(uri: str) -> Any:
    parsed = urlparse(uri)
    if parsed.scheme == "s3":
        bucket = parsed.netloc
        key = parsed.path[1:]
        obj = S3_CLIENT.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read().decode("utf-8")

    return STAC_IO.default_read_text_method(uri)


def write_method(uri: str, txt: Union[bytes, IO[bytes]]) -> None:
    parsed = urlparse(uri)
    if parsed.scheme == "s3":
        bucket = parsed.netloc
        key = parsed.path[1:]
        S3_CLIENT.put_object(Bucket=bucket, Key=key, Body=txt)
    else:
        STAC_IO.default_write_text_method(uri, txt)
