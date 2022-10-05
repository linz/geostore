import hashlib
from logging import Logger
from os.path import basename
from typing import Callable, Optional, Tuple
from urllib.parse import urlparse

from botocore.exceptions import ClientError
from botocore.response import StreamingBody

from .logging_keys import GIT_COMMIT
from .parameter_store import ParameterName, get_param
from .resources import Resource
from .s3 import get_s3_client_for_role

KNOWN_ETAG_OF_EMPTY_FILE = '"d41d8cd98f00b204e9800998ecf8427e"'


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


def get_s3_etag(s3_bucket: str, s3_object_key: str, logger: Logger) -> Optional[str]:
    geostore_s3_client = get_s3_client_for_role(get_param(ParameterName.S3_USERS_ROLE_ARN))

    try:
        s3_response = geostore_s3_client.head_object(Bucket=s3_bucket, Key=s3_object_key)
        return s3_response["ETag"]
    except ClientError as error:
        if error.response["Error"]["Code"] != "404":
            logger.debug(
                f"Unable to fetch eTag for “{s3_object_key}” in s3://{s3_bucket} due to “{error}”",
                extra={GIT_COMMIT: get_param(ParameterName.GIT_COMMIT)},
            )
        # rather than raise, we return an empty string, indicating that the etag is different
        # thus allowing the next step to continue rather than stalling the entire process
        return None


def calculate_s3_etag(body: bytes) -> str:
    # https://awscli.amazonaws.com/v2/documentation/api/latest/topic/s3-config.html#multipart-chunksize
    s3_default_chunk_size = 8_388_608  # Default value is 8 * 1024 * 1024

    if body == b"":
        return KNOWN_ETAG_OF_EMPTY_FILE

    chunk_hashes = []

    for chunk_start in range(0, len(body), s3_default_chunk_size):
        chunk = body[chunk_start : chunk_start + s3_default_chunk_size]
        chunk_hashes.append(hashlib.md5(chunk))

    # file smaller than s3_default_chunk_size has one chunk
    if len(chunk_hashes) == 1:
        # file at exactly s3_default_chunk_size is still one chunk
        # but etag is calculated as multi chunk file (e.g. "656dadd6d61e0ebfd29264e34d742df3-1")
        # where -1 suffix signifies 1 chunk
        if len(body) < s3_default_chunk_size:
            return f'"{chunk_hashes[0].hexdigest()}"'

    hash_object = hashlib.md5()
    for chunk_hash in chunk_hashes:
        hash_object.update(chunk_hash.digest())

    return f'"{hash_object.hexdigest()}-{len(chunk_hashes)}"'
