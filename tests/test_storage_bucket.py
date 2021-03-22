"""
Data Lake Storage Bucket tests.
"""

from mypy_boto3_s3 import S3Client
from mypy_boto3_ssm import SSMClient
from pytest import mark

from backend.parameter_store import ParameterName, get_param


class TestWithStorageBucket:
    storage_bucket_name: str

    @classmethod
    def setup_class(cls) -> None:
        cls.storage_bucket_name = get_param(ParameterName.STORAGE_BUCKET_NAME)

    @mark.infrastructure
    def should_create_storage_bucket_location_constraint(self, s3_client: S3Client) -> None:
        """Test if Data Lake Storage S3 Bucket is created in correct region."""
        response = s3_client.get_bucket_location(Bucket=self.storage_bucket_name)
        assert response["LocationConstraint"] == "ap-southeast-2"

    @mark.infrastructure
    def should_enable_storage_bucket_versioning(self, s3_client: S3Client) -> None:
        """Test if Data Lake Storage S3 Bucket versioning is enabled."""
        response = s3_client.get_bucket_versioning(Bucket=self.storage_bucket_name)
        assert response["Status"] == "Enabled"

    @mark.infrastructure
    def should_create_storage_bucket_public_access_block(self, s3_client: S3Client) -> None:
        """Test if Data Lake Storage S3 Bucket access is blocked for public."""
        response = s3_client.get_public_access_block(Bucket=self.storage_bucket_name)
        assert response["PublicAccessBlockConfiguration"]["BlockPublicAcls"] is True
        assert response["PublicAccessBlockConfiguration"]["IgnorePublicAcls"] is True
        assert response["PublicAccessBlockConfiguration"]["BlockPublicPolicy"] is True
        assert response["PublicAccessBlockConfiguration"]["RestrictPublicBuckets"] is True


@mark.infrastructure
def should_create_storage_bucket_name_parameter(ssm_client: SSMClient) -> None:
    """Test if Data Lake Storage Bucket ARN Parameter was created"""
    parameter_response = ssm_client.get_parameter(Name=ParameterName.STORAGE_BUCKET_NAME.value)
    assert parameter_response["Parameter"]["Name"] == ParameterName.STORAGE_BUCKET_NAME.value
