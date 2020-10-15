"""
Data Lake AWS resources definitions.
"""

from aws_cdk import aws_dynamodb, aws_lambda, aws_s3, core
from aws_cdk.core import Tags


class DataLakeStack(core.Stack):
    """Data Lake stack definition."""

    # pylint: disable=redefined-builtin
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        ENV = self.stack_name.split("-")[-1]

        # set resources removal policy for different environments
        if ENV == "prod":
            REMOVAL_POLICY = core.RemovalPolicy.RETAIN
        else:
            REMOVAL_POLICY = core.RemovalPolicy.DESTROY

        # Data Lake Storage S3 Bucket
        datalake = aws_s3.Bucket(
            self,
            "data-lake-storage-bucket",
            bucket_name="{}-{}".format(
                self.node.try_get_context("data-lake-storage-bucket-name"), ENV
            ),
            access_control=aws_s3.BucketAccessControl.PRIVATE,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=REMOVAL_POLICY,
        )
        Tags.of(datalake).add("ApplicationLayer", "storage")

        # Data Lake Application DB
        db_datasets_table = aws_dynamodb.Table(
            self,
            "data-lake-application-db",
            table_name="datasets",
            partition_key=aws_dynamodb.Attribute(
                name="id", type=aws_dynamodb.AttributeType("STRING")
            ),
            sort_key=aws_dynamodb.Attribute(name="type", type=aws_dynamodb.AttributeType("STRING")),
            point_in_time_recovery=True,
            removal_policy=REMOVAL_POLICY,
        )
        Tags.of(db_datasets_table).add("ApplicationLayer", "application-db")

        # Lambda Handler Functions
        dataset_handler_function = aws_lambda.Function(
            self,
            "dataset-handler-function",
            handler="function.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_6,
            code=aws_lambda.Code.from_asset(path="lambda/dataset-handler"),
        )
        db_datasets_table.grant_read_write_data(dataset_handler_function)
        Tags.of(dataset_handler_function).add("ApplicationLayer", "api")
