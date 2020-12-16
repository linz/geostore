"""
Data Lake AWS resources definitions.
"""
from aws_cdk import aws_dynamodb, aws_s3, core
from aws_cdk.core import Tags


class StorageStack(core.Stack):
    def __init__(self, scope: core.Construct, stack_id: str, deploy_env, **kwargs) -> None:
        super().__init__(scope, stack_id, **kwargs)

        # set resources removal policy for different environments
        if deploy_env == "prod":
            REMOVAL_POLICY = core.RemovalPolicy.RETAIN
        else:
            REMOVAL_POLICY = core.RemovalPolicy.DESTROY

        ############################################################################################
        # ### STORAGE S3 BUCKET ####################################################################
        ############################################################################################
        self.storage_bucket = aws_s3.Bucket(
            self,
            "storage-bucket",
            bucket_name="{}-{}".format(
                self.node.try_get_context("data-lake-storage-bucket-name"), deploy_env
            ),
            access_control=aws_s3.BucketAccessControl.PRIVATE,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=REMOVAL_POLICY,
        )
        Tags.of(self.storage_bucket).add("ApplicationLayer", "storage")

        ############################################################################################
        # ### APPLICATION DB #######################################################################
        ############################################################################################
        self.datasets_table = aws_dynamodb.Table(
            self,
            "application-db",
            table_name="datasets",
            partition_key=aws_dynamodb.Attribute(name="pk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            point_in_time_recovery=True,
            removal_policy=REMOVAL_POLICY,
        )

        self.datasets_table.add_global_secondary_index(
            index_name="datasets_title",
            partition_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(name="title", type=aws_dynamodb.AttributeType.STRING),
        )
        self.datasets_table.add_global_secondary_index(
            index_name="datasets_owning_group",
            partition_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(
                name="owning_group", type=aws_dynamodb.AttributeType.STRING
            ),
        )

        Tags.of(self.datasets_table).add("ApplicationLayer", "application-db")
