from typing import TYPE_CHECKING, Any, Union

import boto3
from pystac import Link, StacIO

from .boto3_config import CONFIG
from .s3_utils import get_bucket_and_key_from_url

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_s3 import S3Client
else:
    # In production we want to avoid depending on a package which has no runtime impact
    S3Client = object

S3_CLIENT: S3Client = boto3.client("s3", config=CONFIG)


class S3StacIO(StacIO):  # type: ignore[misc] # https://github.com/stac-utils/pystac/issues/582
    def read_text(self, source: Union[str, Link], *_args: Any, **_kwargs: Any) -> str:
        bucket, key = get_bucket_and_key_from_url(source)
        obj = S3_CLIENT.get_object(Bucket=bucket, Key=key)
        result: str = obj["Body"].read().decode("utf-8")

        return result

    def write_text(self, dest: Union[str, Link], txt: str, *_args: Any, **_kwargs: Any) -> None:
        bucket, key = get_bucket_and_key_from_url(dest)
        S3_CLIENT.put_object(Bucket=bucket, Key=key, Body=txt.encode())
