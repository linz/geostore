import hashlib
from logging import Logger
from os.path import basename
from typing import Callable, Tuple
from urllib.parse import urlparse

import boto3
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


def check_if_s3_object_exists(bucket: str, key: str) -> bool:
    s3_resource = boto3.resource("s3")

    try:
        s3_resource.Object(bucket, key).load()
    except ClientError:
        # 404 is most likely the response code here
        # other response code is possible, but we return false rather than raise,
        # thus allowing the next step to continue rather than stalling the entire process
        return False

    return True


def get_s3_etag(s3_bucket: str, s3_object_key: str, logger: Logger) -> str:
    s3_client = boto3.client("s3")

    try:
        s3_response = s3_client.head_object(Bucket=s3_bucket, Key=s3_object_key)
        s3_etag = s3_response["ETag"].strip('"')
        return s3_etag
    except ClientError as error:
        logger.error(
            f"Unable to fetch eTag for {s3_object_key} in s3://{s3_bucket},"
            f"due to {error}, even though object exists in bucket.",
            extra={GIT_COMMIT: get_param(ParameterName.GIT_COMMIT)},
        )
        # rather than raise, we return an empty string, indicating that the etag is different
        # thus allowing the next step to continue rather than stalling the entire process
        return ""


def calculate_s3_etag(
    body: bytes,
    # https://docs.aws.amazon.com/cli/latest/topic/s3-config.html#multipart-chunksize
    chunk_size: int = 8 * 1024 * 1024,  # Default value
) -> str:

    md5s = []
    data = [body[i : i + chunk_size] for i in range(0, len(body), chunk_size)]

    for chunk in data:
        md5s.append(hashlib.md5(chunk))

    if len(md5s) < 1:
        return f"{hashlib.md5().hexdigest()}"

    if len(md5s) == 1:
        return f"{md5s[0].hexdigest()}"

    digests = b"".join(m.digest() for m in md5s)
    digests_md5 = hashlib.md5(digests)
    return f"{digests_md5.hexdigest()}-{len(md5s)}"
