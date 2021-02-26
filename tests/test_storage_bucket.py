"""
Data Lake Storage Bucket tests.
"""

from mypy_boto3_s3 import S3Client
from pytest import mark

from backend.processing.utils import ResourceName


@mark.infrastructure
def test_storage_bucket_location(s3_client: S3Client) -> None:
    """Test if Data Lake Storage S3 Bucket is created in correct region."""
    response = s3_client.get_bucket_location(Bucket=ResourceName.STORAGE_BUCKET_NAME.value)
    assert response["LocationConstraint"] == "ap-southeast-2"


@mark.infrastructure
def test_storage_bucket_versioning(s3_client: S3Client) -> None:
    """Test if Data Lake Storage S3 Bucket versioning is enabled."""
    response = s3_client.get_bucket_versioning(Bucket=ResourceName.STORAGE_BUCKET_NAME.value)
    assert response["Status"] == "Enabled"


@mark.infrastructure
def test_storage_bucket_public_access_block(s3_client: S3Client) -> None:
    """Test if Data Lake Storage S3 Bucket access is blocked for public."""
    response = s3_client.get_public_access_block(Bucket=ResourceName.STORAGE_BUCKET_NAME.value)
    assert response["PublicAccessBlockConfiguration"]["BlockPublicAcls"] is True
    assert response["PublicAccessBlockConfiguration"]["IgnorePublicAcls"] is True
    assert response["PublicAccessBlockConfiguration"]["BlockPublicPolicy"] is True
    assert response["PublicAccessBlockConfiguration"]["RestrictPublicBuckets"] is True
