"""
Pytest configuration file.
"""

import logging

import boto3
import pytest
from mypy_boto3_batch import BatchClient
from mypy_boto3_dynamodb import DynamoDBClient
from mypy_boto3_lambda import LambdaClient
from mypy_boto3_s3 import S3Client
from mypy_boto3_ssm import SSMClient
from mypy_boto3_stepfunctions import SFNClient

from ..processing.model import DatasetModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture()
def batch_client() -> BatchClient:
    return boto3.client("batch")


@pytest.fixture()
def dynamodb_client() -> DynamoDBClient:
    return boto3.client("dynamodb")


@pytest.fixture()
def lambda_client() -> LambdaClient:
    return boto3.client("lambda")


@pytest.fixture()
def s3_client() -> S3Client:
    return boto3.client("s3")


@pytest.fixture()
def ssm_client() -> SSMClient:
    return boto3.client("ssm")


@pytest.fixture()
def step_functions_client() -> SFNClient:
    return boto3.client("stepfunctions")


@pytest.fixture()
def db_teardown() -> None:
    logger.debug("Removing all dataset instances before test")

    for item in DatasetModel.scan():
        item.delete()
