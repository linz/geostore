"""
Data Lake Storage Bucket tests.
"""

import os

import boto3

S3 = boto3.client("s3")


if "ENVIRONMENT_TYPE" in os.environ:
    ENV = os.environ["ENVIRONMENT_TYPE"]
else:
    ENV = "dev"


def test_storage_bucket_region():
    """Test if Data Lake Storage S3 Bucket is created in correct region."""
    response = S3.get_bucket_location(Bucket=f"linz-geospatial-data-lake-{ENV}")
    assert response["LocationConstraint"] == "ap-southeast-2"


def test_storage_bucket_versioning():
    """Test if Data Lake Storage S3 Bucket versioning is enabled."""
    response = S3.get_bucket_versioning(
        Bucket=f"linz-geospatial-data-lake-{ENV}"
    )
    assert response["Status"] == "Enabled"
