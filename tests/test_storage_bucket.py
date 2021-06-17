from mypy_boto3_s3 import S3Client
from pytest import mark

from backend.resources import ResourceName


@mark.infrastructure
def should_create_storage_bucket_location_constraint(s3_client: S3Client) -> None:
    """Test if Geostore Storage S3 Bucket is created in correct region."""
    response = s3_client.get_bucket_location(Bucket=ResourceName.STORAGE_BUCKET_NAME.value)
    assert response["LocationConstraint"] == "ap-southeast-2"


@mark.infrastructure
def should_enable_storage_bucket_versioning(s3_client: S3Client) -> None:
    """Test if Geostore Storage S3 Bucket versioning is enabled."""
    response = s3_client.get_bucket_versioning(Bucket=ResourceName.STORAGE_BUCKET_NAME.value)
    assert response["Status"] == "Enabled"


@mark.infrastructure
def should_create_storage_bucket_public_access_block(s3_client: S3Client) -> None:
    """Test if Geostore Storage S3 Bucket access is blocked for public."""
    response = s3_client.get_public_access_block(Bucket=ResourceName.STORAGE_BUCKET_NAME.value)
    public_access_block_configuration = response["PublicAccessBlockConfiguration"]
    assert public_access_block_configuration["BlockPublicAcls"] is True
    assert public_access_block_configuration["IgnorePublicAcls"] is True
    assert public_access_block_configuration["BlockPublicPolicy"] is True
    assert public_access_block_configuration["RestrictPublicBuckets"] is True
