from typing import Any

from aws_cdk import aws_s3, aws_ssm
from aws_cdk.core import Construct, RemovalPolicy, Stack, Tags

from backend.parameter_store import ParameterName


class StagingStack(Stack):
    def __init__(self, scope: Construct, stack_id: str, deploy_env: str, **kwargs: Any) -> None:
        super().__init__(scope, stack_id, **kwargs)

        ############################################################################################
        # ### DATASET STAGING S3 BUCKET ############################################################
        ############################################################################################
        self.staging_bucket = aws_s3.Bucket(
            self,
            "dataset-staging-bucket",
            access_control=aws_s3.BucketAccessControl.PRIVATE,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        Tags.of(self.staging_bucket).add("ApplicationLayer", "storage")  # type: ignore[arg-type]

        self.staging_bucket_name_parameter = aws_ssm.StringParameter(
            self,
            "staging bucket name",
            description=f"Staging Bucket name for {deploy_env}",
            parameter_name=ParameterName.STAGING_BUCKET_NAME.value,
            string_value=self.staging_bucket.bucket_name,
        )
