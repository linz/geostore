from typing import TYPE_CHECKING, Callable, Tuple
from urllib.parse import urlparse

from geostore.s3 import get_s3_client_for_role
from geostore.types import JsonObject

if TYPE_CHECKING:
    from mypy_boto3_s3.type_defs import GetObjectOutputTypeDef
else:
    GetObjectOutputTypeDef = JsonObject  # pragma: no mutate


def get_bucket_and_key_from_url(url: str) -> Tuple[str, str]:
    parsed = urlparse(url)
    return parsed.netloc, parsed.path[1:]


def get_s3_url_reader(s3_role_arn: str) -> Callable[[str], GetObjectOutputTypeDef]:
    def s3_url_reader(url: str) -> GetObjectOutputTypeDef:
        bucket_name, key = get_bucket_and_key_from_url(url)

        url_object = staging_s3_client.get_object(Bucket=bucket_name, Key=key)
        return url_object

    staging_s3_client = get_s3_client_for_role(s3_role_arn)
    return s3_url_reader
