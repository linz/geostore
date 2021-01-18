from aws_cdk import aws_s3, core
from aws_cdk.core import Tags


class DatasetSourceStack(core.Stack):
    def __init__(self, scope: core.Construct, stack_id: str, deploy_env, **kwargs) -> None:
        super().__init__(scope, stack_id, **kwargs)

        ############################################################################################
        # ### DATASET SOURCE S3 BUCKET #############################################################
        ############################################################################################
        self.source_bucket = aws_s3.Bucket(
            self,
            "dataset-source-bucket",
            bucket_name="{}-{}".format(
                self.node.try_get_context("data-lake-dataset-source-bucket-name"), deploy_env
            ),
            access_control=aws_s3.BucketAccessControl.PRIVATE,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=core.RemovalPolicy.DESTROY,
        )
        Tags.of(self.source_bucket).add("ApplicationLayer", "storage")
