from typing import IO, Any, Union
from urllib.parse import urlparse

import boto3
from pystac import STAC_IO  # type: ignore[import]


def read_method(uri: str) -> Any:
    parsed = urlparse(uri)
    if parsed.scheme == "s3":
        bucket = parsed.netloc
        key = parsed.path[1:]
        s3_resource = boto3.resource("s3")
        obj = s3_resource.Object(bucket, key)
        return obj.get()["Body"].read().decode("utf-8")

    return STAC_IO.default_read_text_method(uri)


def write_method(uri: str, txt: Union[bytes, IO[bytes]]) -> None:
    parsed = urlparse(uri)
    if parsed.scheme == "s3":
        bucket = parsed.netloc
        key = parsed.path[1:]
        s3_resource = boto3.resource("s3")
        s3_resource.Object(bucket, key).put(Body=txt)
    else:
        STAC_IO.default_write_text_method(uri, txt)
