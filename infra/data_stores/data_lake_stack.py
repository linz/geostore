"""
Data Lake AWS resources definitions.
"""

from aws_cdk import aws_s3, core


class DataLakeStack(core.Stack):
    """Data Lake stack definition."""

    # pylint: disable=redefined-builtin
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        env = self.stack_name.split("-")[-1]

        # S3 buckets removal policy
        if env == "prod":
            removal_policy = core.RemovalPolicy.RETAIN
        else:
            removal_policy = core.RemovalPolicy.DESTROY

        # The datalake s3 bucket
        # pylint: disable=unused-variable #temp datalake variable to be used

        datalake = aws_s3.Bucket(
            self,
            "data-lake-storage-bucket",
            bucket_name=f"linz-geospatial-data-lake-{env}",
            access_control=aws_s3.BucketAccessControl.PRIVATE,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=removal_policy,
        )
