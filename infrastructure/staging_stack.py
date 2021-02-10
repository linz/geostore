from typing import Any

from aws_cdk import aws_s3, aws_ssm, core
from aws_cdk.core import Tags

from backend.utils import ENV, ResourceName

STAGING_BUCKET_PARAMETER = f"/{ENV}/staging-bucket-arn"


class StagingStack(core.Stack):
    def __init__(
        self, scope: core.Construct, stack_id: str, deploy_env: str, **kwargs: Any
    ) -> None:
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
        Tags.of(self.staging_bucket).add("ApplicationLayer", "storage")  # type: ignore[arg-type]

        aws_ssm.StringParameter(
            self,
            "staging-bucket-arn",
            description=f"Staging Bucket ARN for {deploy_env}",
            parameter_name=STAGING_BUCKET_PARAMETER,
            string_value=self.staging_bucket.bucket_arn,
        )
