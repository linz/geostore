"""
Data Lake AWS resources definitions.
"""
from aws_cdk import aws_dynamodb, aws_lambda, aws_s3, core
from aws_cdk.core import Tags


class DataLakeStack(core.Stack):
    """Data Lake stack definition."""

    # pylint: disable=redefined-builtin,too-many-locals
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        ENV = self.stack_name.split("-")[-1]

        # set resources removal policy for different environments
        if ENV == "prod":
            REMOVAL_POLICY = core.RemovalPolicy.RETAIN
        else:
            REMOVAL_POLICY = core.RemovalPolicy.DESTROY

        ############################################################################################
        # ### STORAGE S3 BUCKET ####################################################################
        ############################################################################################
        storage_bucket = aws_s3.Bucket(
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
        Tags.of(storage_bucket).add("ApplicationLayer", "storage")

        ############################################################################################
        # ### APPLICATION DB #######################################################################
        ############################################################################################
        app_db_datasets = aws_dynamodb.Table(
            self,
            "data-lake-application-db",
            table_name="datasets",
            partition_key=aws_dynamodb.Attribute(name="pk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            point_in_time_recovery=True,
            removal_policy=REMOVAL_POLICY,
        )

        app_db_datasets.add_global_secondary_index(
            index_name="datasets_title",
            partition_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(name="title", type=aws_dynamodb.AttributeType.STRING),
        )
        app_db_datasets.add_global_secondary_index(
            index_name="datasets_owning_group",
            partition_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(
                name="owning_group", type=aws_dynamodb.AttributeType.STRING
            ),
        )

        Tags.of(app_db_datasets).add("ApplicationLayer", "application-db")

        ############################################################################################
        # ### API ENDPOINTS ########################################################################
        ############################################################################################

        endpoints = ("datasets",)

        for endpoint in endpoints:
            endpoint_function = aws_lambda.Function(
                self,
                f"{endpoint}-endpoint-function",
                function_name=f"{endpoint}-endpoint",
                handler=f"endpoints.{endpoint}.entrypoint.lambda_handler",
                runtime=aws_lambda.Runtime.PYTHON_3_6,
                code=aws_lambda.Code.from_asset(
                    path="..",
                    bundling=core.BundlingOptions(
                        # pylint:disable=no-member
                        image=aws_lambda.Runtime.PYTHON_3_6.bundling_docker_image,
                        command=["backend/endpoints/bundle.bash", f"{endpoint}"],
                    ),
                ),
            )

            app_db_datasets.grant_read_write_data(endpoint_function)
            app_db_datasets.grant(
                endpoint_function, "dynamodb:DescribeTable"
            )  # required by pynamodb

            Tags.of(endpoint_function).add("ApplicationLayer", "api")
