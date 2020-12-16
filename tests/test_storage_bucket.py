"""
Data Lake Storage Bucket tests.
"""

import os

from pytest import mark

ENV = os.environ.get("DEPLOY_ENV", "dev")


@mark.infrastructure
def test_storage_bucket_location(s3_client):
    """Test if Data Lake Storage S3 Bucket is created in correct region."""
    response = s3_client.get_bucket_location(Bucket=f"linz-geospatial-data-lake-{ENV}")
    assert response["LocationConstraint"] == "ap-southeast-2"


@mark.infrastructure
def test_storage_bucket_versioning(s3_client):
    """Test if Data Lake Storage S3 Bucket versioning is enabled."""
    response = s3_client.get_bucket_versioning(Bucket=f"linz-geospatial-data-lake-{ENV}")
    assert response["Status"] == "Enabled"


@mark.infrastructure
def test_storage_bucket_public_access_block(s3_client):
    """Test if Data Lake Storage S3 Bucket access is blocked for public."""
    response = s3_client.get_public_access_block(Bucket=f"linz-geospatial-data-lake-{ENV}")
    assert response["PublicAccessBlockConfiguration"]["BlockPublicAcls"] is True
    assert response["PublicAccessBlockConfiguration"]["IgnorePublicAcls"] is True
    assert response["PublicAccessBlockConfiguration"]["BlockPublicPolicy"] is True
    assert response["PublicAccessBlockConfiguration"]["RestrictPublicBuckets"] is True
