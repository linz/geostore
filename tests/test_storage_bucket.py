"""
Geostore Storage Bucket tests.
"""

from mypy_boto3_s3 import S3Client
from pytest import mark

from backend.resources import ResourceName


class TestWithStorageBucket:
    storage_bucket_name: str

    @classmethod
    def setup_class(cls) -> None:
        cls.storage_bucket_name = ResourceName.STORAGE_BUCKET_NAME.value

    @mark.infrastructure
    def should_create_storage_bucket_location_constraint(self, s3_client: S3Client) -> None:
        """Test if Geostore Storage S3 Bucket is created in correct region."""
        response = s3_client.get_bucket_location(Bucket=self.storage_bucket_name)
        assert response["LocationConstraint"] == "ap-southeast-2"

    @mark.infrastructure
    def should_enable_storage_bucket_versioning(self, s3_client: S3Client) -> None:
        """Test if Geostore Storage S3 Bucket versioning is enabled."""
        response = s3_client.get_bucket_versioning(Bucket=self.storage_bucket_name)
        assert response["Status"] == "Enabled"

    @mark.infrastructure
    def should_create_storage_bucket_public_access_block(self, s3_client: S3Client) -> None:
        """Test if Geostore Storage S3 Bucket access is blocked for public."""
        response = s3_client.get_public_access_block(Bucket=self.storage_bucket_name)
        assert response["PublicAccessBlockConfiguration"]["BlockPublicAcls"] is True
        assert response["PublicAccessBlockConfiguration"]["IgnorePublicAcls"] is True
        assert response["PublicAccessBlockConfiguration"]["BlockPublicPolicy"] is True
        assert response["PublicAccessBlockConfiguration"]["RestrictPublicBuckets"] is True
