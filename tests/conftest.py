import boto3
import pytest


@pytest.fixture()
def s3_client():
    return boto3.client("s3")


@pytest.fixture()
def lambda_client():
    return boto3.client("lambda")
