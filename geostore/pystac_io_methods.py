from logging import Logger
from typing import TYPE_CHECKING, Any, Union

import boto3
from linz_logger import get_log
from pystac.link import Link
from pystac.stac_io import StacIO

from .boto3_config import CONFIG
from .s3_utils import (
    calculate_s3_etag,
    check_if_s3_object_exists,
    get_bucket_and_key_from_url,
    get_s3_etag,
)

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_s3 import S3Client
else:
    # In production we want to avoid depending on a package which has no runtime impact
    S3Client = object  # pragma: no mutate

S3_CLIENT: S3Client = boto3.client("s3", config=CONFIG)
LOGGER: Logger = get_log()


class S3StacIO(StacIO):
    def read_text(  # type: ignore[override]
        self, source: Union[str, Link], *_args: Any, **_kwargs: Any
    ) -> str:
        url = source.href if isinstance(source, Link) else source
        bucket, key = get_bucket_and_key_from_url(url)
        obj = S3_CLIENT.get_object(Bucket=bucket, Key=key)
        result: str = obj["Body"].read().decode("utf-8")

        return result

    def write_text(  # type: ignore[override]
        self, dest: Union[str, Link], txt: str, *_args: Any, **_kwargs: Any
    ) -> None:
        url = dest.href if isinstance(dest, Link) else dest
        bucket, key = get_bucket_and_key_from_url(url)
        if check_if_s3_object_exists(bucket, key):
            s3_etag = get_s3_etag(bucket, key, LOGGER)
            local_etag = calculate_s3_etag(txt.encode())

            if s3_etag == local_etag:
                return

        S3_CLIENT.put_object(Bucket=bucket, Key=key, Body=txt.encode())
