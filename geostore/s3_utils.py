from typing import Callable, Tuple
from urllib.parse import urlparse

from botocore.response import StreamingBody

from geostore.s3 import get_s3_client_for_role


def get_bucket_and_key_from_url(url: str) -> Tuple[str, str]:
    parsed = urlparse(url)
    return parsed.netloc, parsed.path[1:]


def get_s3_url_reader(s3_role_arn: str) -> Callable[[str], StreamingBody]:
    def s3_url_reader(url: str) -> StreamingBody:
        bucket_name, key = get_bucket_and_key_from_url(url)

        url_object = staging_s3_client.get_object(Bucket=bucket_name, Key=key)
        return url_object["Body"]

    staging_s3_client = get_s3_client_for_role(s3_role_arn)
    return s3_url_reader
