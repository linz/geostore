from typing import Any

from aws_cdk import aws_s3
from aws_cdk.core import Construct, NestedStack, RemovalPolicy, Tags

from backend.resources import ResourceName


class StagingStack(NestedStack):
    def __init__(self, scope: Construct, stack_id: str, **kwargs: Any) -> None:
        super().__init__(scope, stack_id, **kwargs)

        ############################################################################################
        # ### DATASET STAGING S3 BUCKET ############################################################
        ############################################################################################
        self.staging_bucket = aws_s3.Bucket(
            self,
            "dataset-staging-bucket",
            bucket_name=ResourceName.STAGING_BUCKET_NAME.value,
            access_control=aws_s3.BucketAccessControl.PRIVATE,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        Tags.of(self).add("ApplicationLayer", "staging")  # type: ignore[arg-type]
