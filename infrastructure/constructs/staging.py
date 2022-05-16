from aws_cdk import RemovalPolicy, Tags, aws_iam, aws_s3
from constructs import Construct

from geostore.resources import Resource


class Staging(Construct):
    def __init__(self, scope: Construct, stack_id: str, *, users_role: aws_iam.Role) -> None:
        super().__init__(scope, stack_id)

        ############################################################################################
        # ### DATASET STAGING S3 BUCKET ############################################################
        ############################################################################################
        staging_bucket = aws_s3.Bucket(
            self,
            "dataset-staging-bucket",
            bucket_name=Resource.STAGING_BUCKET_NAME.resource_name,
            access_control=aws_s3.BucketAccessControl.PRIVATE,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        staging_bucket.grant_read(users_role)

        Tags.of(self).add("ApplicationLayer", "staging")
