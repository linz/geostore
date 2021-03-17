from typing import Any

from aws_cdk import aws_s3
from aws_cdk.core import Construct, RemovalPolicy, Stack, Tags

from backend.environment import ENV
from backend.resources import ResourceName

STAGING_BUCKET_PARAMETER = f"/{ENV}/staging-bucket-arn"


class StagingStack(Stack):
    def __init__(self, scope: Construct, stack_id: str, **kwargs: Any) -> None:
        super().__init__(scope, stack_id, **kwargs)

        ############################################################################################
        # ### DATASET STAGING S3 BUCKET ############################################################
        ############################################################################################
        self.staging_bucket = aws_s3.Bucket(
            self,
            "dataset-staging-bucket",
            bucket_name=ResourceName.DATASET_STAGING_BUCKET_NAME.value,
            access_control=aws_s3.BucketAccessControl.PRIVATE,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        Tags.of(self.staging_bucket).add("ApplicationLayer", "storage")  # type: ignore[arg-type]
