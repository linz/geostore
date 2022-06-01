from typing import Callable, Tuple
from urllib.parse import urlparse

from botocore.response import StreamingBody

from .s3 import get_s3_client_for_role


def get_bucket_and_key_from_url(url: str) -> Tuple[str, str]:
    parsed = urlparse(url)
    return parsed.netloc, parsed.path[1:]


class GeostoreS3Response:
    # pylint: disable=too-few-public-methods
    def __init__(self, response: StreamingBody):
        self.response = response


def get_s3_url_reader(s3_role_arn: str) -> Callable[[str], GeostoreS3Response]:
    def s3_url_reader(url: str) -> GeostoreS3Response:
        bucket_name, key = get_bucket_and_key_from_url(url)

        url_object = staging_s3_client.get_object(Bucket=bucket_name, Key=key)
        response = GeostoreS3Response(url_object["Body"])

        return response

    staging_s3_client = get_s3_client_for_role(s3_role_arn)
    return s3_url_reader
