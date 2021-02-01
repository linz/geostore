from aws_cdk import aws_s3, core
from aws_cdk.core import Tags

from datalake.backend.endpoints.utils import ResourceName


class StagingStack(core.Stack):
    def __init__(self, scope: core.Construct, stack_id: str, **kwargs) -> None:
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
            removal_policy=core.RemovalPolicy.DESTROY,
        )
        Tags.of(self.staging_bucket).add("ApplicationLayer", "storage")
