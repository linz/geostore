import hashlib
from logging import Logger
from os.path import basename
from typing import Callable, Tuple
from urllib.parse import urlparse

from botocore.exceptions import ClientError
from botocore.response import StreamingBody

from .logging_keys import GIT_COMMIT
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
    s3_role_arn: str, dataset_title: str, logger: Logger
) -> Callable[[str], GeostoreS3Response]:
    def s3_url_reader(staging_url: str) -> GeostoreS3Response:
        bucket_name, key = get_bucket_and_key_from_url(staging_url)

        try:
            staging_object = staging_s3_client.get_object(Bucket=bucket_name, Key=key)
            return GeostoreS3Response(staging_object["Body"], True)
        except ClientError as error:
            if error.response["Error"]["Code"] != "NoSuchKey":
                raise error

            geostore_key = f"{dataset_title}/{basename(urlparse(staging_url).path[1:])}"

            logger.debug(
                f"'{key}' is not present in the staging bucket."
                f" Using '{geostore_key}' from the geostore bucket for validation instead.",
                extra={GIT_COMMIT: get_param(ParameterName.GIT_COMMIT)},
            )
            geostore_object = geostore_s3_client.get_object(
                Bucket=Resource.STORAGE_BUCKET_NAME.resource_name, Key=geostore_key
            )
            return GeostoreS3Response(geostore_object["Body"], False)

    staging_s3_client = get_s3_client_for_role(s3_role_arn)
    geostore_s3_client = get_s3_client_for_role(get_param(ParameterName.S3_USERS_ROLE_ARN))
    return s3_url_reader


def get_s3_etag(s3_bucket: str, s3_object_key: str, logger: Logger) -> str:
    geostore_s3_client = get_s3_client_for_role(get_param(ParameterName.S3_USERS_ROLE_ARN))

    try:
        s3_response = geostore_s3_client.get_object(Bucket=s3_bucket, Key=s3_object_key)
        return s3_response["ETag"]
    except ClientError as error:
        if error.response["Error"]["Code"] != "NoSuchKey":
            logger.debug(
                f"Unable to fetch eTag for “{s3_object_key}” in s3://{s3_bucket} due to “{error}”",
                extra={GIT_COMMIT: get_param(ParameterName.GIT_COMMIT)},
            )
        # rather than raise, we return an empty string, indicating that the etag is different
        # thus allowing the next step to continue rather than stalling the entire process
        return ""


def calculate_s3_etag(
    body: bytes,
    # https://awscli.amazonaws.com/v2/documentation/api/latest/topic/s3-config.html#multipart-chunksize
    chunk_size: int = 8 * 1024 * 1024,  # Default value
) -> str:

    if body == b"":
        return f'"{hashlib.md5().hexdigest()}"'

    chunk_hashes = []

    for chunk_start in range(0, len(body), chunk_size):
        chunk = body[chunk_start : chunk_start + chunk_size]
        chunk_hashes.append(hashlib.md5(chunk))

    if len(chunk_hashes) == 1:
        return f'"{chunk_hashes[0].hexdigest()}"'

    hash_object = hashlib.md5()
    for chunk_hash in chunk_hashes:
        hash_object.update(chunk_hash.digest())

    return f'"{hash_object.hexdigest()}-{len(chunk_hashes)}"'
