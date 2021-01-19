"""
Pytest configuration file.
"""

import logging

import boto3
import pytest

from ..endpoints.datasets.model import DatasetModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture()
def batch_client():
    return boto3.client("batch")


@pytest.fixture()
def lambda_client():
    return boto3.client("lambda")


@pytest.fixture()
def s3_client():
    return boto3.client("s3")


@pytest.fixture()
def stepfunctions_client():
    return boto3.client("stepfunctions")


@pytest.fixture()
def db_teardown():
    logger.debug("Removing all dataset instances before test")

    for item in DatasetModel.scan():
        item.delete()

    yield

    return True
