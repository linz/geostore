from aws_cdk import aws_iam, aws_s3
from aws_cdk.core import Construct, RemovalPolicy, Tags

from geostore.resources import ResourceName


class Staging(Construct):
    def __init__(self, scope: Construct, stack_id: str, *, users_role: aws_iam.Role) -> None:
        super().__init__(scope, stack_id)

        ############################################################################################
        # ### DATASET STAGING S3 BUCKET ############################################################
        ############################################################################################
        staging_bucket = aws_s3.Bucket(
            self,
            "dataset-staging-bucket",
            bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
            access_control=aws_s3.BucketAccessControl.PRIVATE,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        staging_bucket.grant_read(users_role)  # type: ignore[arg-type]

        Tags.of(self).add("ApplicationLayer", "staging")  # type: ignore[arg-type]
