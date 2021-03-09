"""
Data Lake Storage Bucket tests.
"""

from mypy_boto3_s3 import S3Client
from mypy_boto3_ssm import SSMClient
from pytest import mark

from backend.import_dataset.task import STORAGE_BUCKET_PARAMETER_NAME
from backend.resources import ResourceName


@mark.infrastructure
def should_create_storage_bucket_location_constraint(s3_client: S3Client) -> None:
    """Test if Data Lake Storage S3 Bucket is created in correct region."""
    response = s3_client.get_bucket_location(Bucket=ResourceName.STORAGE_BUCKET_NAME.value)
    assert response["LocationConstraint"] == "ap-southeast-2"


@mark.infrastructure
def should_enable_storage_bucket_versioning(s3_client: S3Client) -> None:
    """Test if Data Lake Storage S3 Bucket versioning is enabled."""
    response = s3_client.get_bucket_versioning(Bucket=ResourceName.STORAGE_BUCKET_NAME.value)
    assert response["Status"] == "Enabled"


@mark.infrastructure
def should_create_storage_bucket_public_access_block(s3_client: S3Client) -> None:
    """Test if Data Lake Storage S3 Bucket access is blocked for public."""
    response = s3_client.get_public_access_block(Bucket=ResourceName.STORAGE_BUCKET_NAME.value)
    assert response["PublicAccessBlockConfiguration"]["BlockPublicAcls"] is True
    assert response["PublicAccessBlockConfiguration"]["IgnorePublicAcls"] is True
    assert response["PublicAccessBlockConfiguration"]["BlockPublicPolicy"] is True
    assert response["PublicAccessBlockConfiguration"]["RestrictPublicBuckets"] is True


@mark.infrastructure
def should_create_storage_bucket_arn_parameter(ssm_client: SSMClient) -> None:
    """Test if Data Lake Storage Bucket ARN Parameter was created"""
    parameter_response = ssm_client.get_parameter(Name=STORAGE_BUCKET_PARAMETER_NAME)
    assert parameter_response["Parameter"]["Name"] == STORAGE_BUCKET_PARAMETER_NAME
    assert "arn" in parameter_response["Parameter"]["Value"]
    assert "s3" in parameter_response["Parameter"]["Value"]
