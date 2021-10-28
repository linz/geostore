from typing import TYPE_CHECKING
from uuid import uuid4

import boto3

from .boto3_config import CONFIG
from .environment import environment_name

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_sts import STSClient
else:
    S3Client = STSClient = object  # pragma: no mutate


S3_SCHEMA = "s3"
S3_URL_PREFIX = f"{S3_SCHEMA}://"

CHUNK_SIZE = 1024

STS_CLIENT: STSClient = boto3.client("sts", config=CONFIG)


def get_s3_client_for_role(role_arn: str) -> S3Client:
    assume_role_response = STS_CLIENT.assume_role(
        RoleArn=role_arn, RoleSessionName=f"{environment_name()}_Geostore_{uuid4()}"
    )
    credentials = assume_role_response["Credentials"]
    client: S3Client = boto3.client(
        "s3",
        config=CONFIG,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )
    return client
