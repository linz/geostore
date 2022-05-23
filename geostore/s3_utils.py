from logging import Logger
from os.path import basename
from typing import Callable, Tuple
from urllib.parse import urlparse

from botocore.exceptions import ClientError
from botocore.response import StreamingBody

from .parameter_store import ParameterName, get_param
from .resources import Resource
from .s3 import get_s3_client_for_role


def get_bucket_and_key_from_url(url: str) -> Tuple[str, str]:
    parsed = urlparse(url)
    return parsed.netloc, parsed.path[1:]


class GeostoreS3Response:
    # pylint: disable=too-few-public-methods
    def __init__(self, response: StreamingBody, file_in_staging: bool):
        self.response = response
        self.file_in_staging = file_in_staging


def get_s3_url_reader(
    s3_role_arn: str, dataset_prefix: str, logger: Logger
) -> Callable[[str], GeostoreS3Response]:
    def s3_url_reader(url: str) -> GeostoreS3Response:
        bucket_name, key = get_bucket_and_key_from_url(url)

        try:
            url_object = staging_s3_client.get_object(Bucket=bucket_name, Key=key)
            response = GeostoreS3Response(url_object["Body"], True)
        except ClientError as error:
            geostore_key = f"{dataset_prefix}/{basename(urlparse(url).path[1:])}"

            if error.response["Error"]["Code"] != "NoSuchKey":
                raise error

            logger.debug(
                f"'{key}' is not present in the staging bucket."
                f" Using '{geostore_key}' from the geostore bucket for validation instead."
            )
            url_object = geostore_s3_client.get_object(
                Bucket=Resource.STORAGE_BUCKET_NAME.resource_name, Key=geostore_key
            )
            response = GeostoreS3Response(url_object["Body"], False)

        return response

    staging_s3_client = get_s3_client_for_role(s3_role_arn)
    geostore_s3_client = get_s3_client_for_role(get_param(ParameterName.S3_USERS_ROLE_ARN))
    return s3_url_reader
