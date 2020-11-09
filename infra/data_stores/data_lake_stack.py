"""
Data Lake AWS resources definitions.
"""

from aws_cdk import aws_s3, core
from aws_cdk.core import Tags


class DataLakeStack(core.Stack):
    """Data Lake stack definition."""

    # pylint: disable=redefined-builtin
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        env = self.stack_name.split("-")[-1]

        if env == "prod":
            removal_policy = core.RemovalPolicy.RETAIN
        else:
            removal_policy = core.RemovalPolicy.DESTROY

        # Data Lake Storage S3 Bucket
        datalake = aws_s3.Bucket(
            self,
            "data-lake-storage-bucket",
            bucket_name="{}-{}".format(
                self.node.try_get_context("data-lake-storage-bucket-name"), env
            ),
            access_control=aws_s3.BucketAccessControl.PRIVATE,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=removal_policy,
        )
        Tags.of(datalake).add("ApplicationLayer", "data-lake-storage")
