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


def test_storage_bucket_location():
    """Test if Data Lake Storage S3 Bucket is created in correct region."""
    response = S3.get_bucket_location(Bucket=f"linz-geospatial-data-lake-{ENV}")
    assert response["LocationConstraint"] == "ap-southeast-2"


def test_storage_bucket_versioning():
    """Test if Data Lake Storage S3 Bucket versioning is enabled."""
    response = S3.get_bucket_versioning(
        Bucket=f"linz-geospatial-data-lake-{ENV}"
    )
    assert response["Status"] == "Enabled"


def test_storage_bucket_public_access_block():
    """Test if Data Lake Storage S3 Bucket access is blocked for public."""
    response = S3.get_public_access_block(
        Bucket=f"linz-geospatial-data-lake-{ENV}"
    )
    assert response["PublicAccessBlockConfiguration"]["BlockPublicAcls"] is True
    assert response["PublicAccessBlockConfiguration"]["IgnorePublicAcls"] is True
    assert response["PublicAccessBlockConfiguration"]["BlockPublicPolicy"] is True
    assert response["PublicAccessBlockConfiguration"]["RestrictPublicBuckets"] is True
