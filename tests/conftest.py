"""
Pytest Localstack configuration.
"""

import logging

import boto3
import pytest_localstack

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(message)s")
logging.getLogger("boto3").setLevel(logging.CRITICAL)
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("s3transfer").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)

localstack = pytest_localstack.patch_fixture(  # pylint: disable=no-member
    services=["s3"],
    scope="session",
    region_name="ap-southeast-2",
    autouse=True,
    localstack_version="0.11.3",  # latest currently supported LS version
)
