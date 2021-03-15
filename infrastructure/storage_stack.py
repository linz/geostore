"""
Data Lake AWS resources definitions.
"""
from typing import Any

from aws_cdk import aws_dynamodb, aws_s3, aws_ssm, core
from aws_cdk.core import Tags

from backend.parameter_store import ParameterName
from backend.resources import ResourceName

from .constructs.table import Table


class StorageStack(core.Stack):
    def __init__(
        self, scope: core.Construct, stack_id: str, deploy_env: str, **kwargs: Any
    ) -> None:
        super().__init__(scope, stack_id, **kwargs)

        # set resources depending on deployment type
        if deploy_env == "prod":
            resource_removal_policy = core.RemovalPolicy.RETAIN
        else:
            resource_removal_policy = core.RemovalPolicy.DESTROY

        ############################################################################################
        # ### STORAGE S3 BUCKET ####################################################################
        ############################################################################################
        self.storage_bucket = aws_s3.Bucket(
            self,
            "storage-bucket",
            access_control=aws_s3.BucketAccessControl.PRIVATE,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=resource_removal_policy,
        )
        Tags.of(self.storage_bucket).add("ApplicationLayer", "storage")  # type: ignore[arg-type]

        self.storage_bucket_arn_parameter = aws_ssm.StringParameter(
            self,
            "Storage Bucket ARN Parameter",
            description=f"Storage Bucket ARN for {deploy_env}",
            parameter_name=ParameterName.STORAGE_BUCKET_ARN.value,
            string_value=self.storage_bucket.bucket_arn,
        )
        self.storage_bucket_name_parameter = aws_ssm.StringParameter(
            self,
            "Storage Bucket Name Parameter",
            description=f"Storage Bucket name for {deploy_env}",
            parameter_name=ParameterName.STORAGE_BUCKET_NAME.value,
            string_value=self.storage_bucket.bucket_name,
        )
        ############################################################################################
        # ### APPLICATION DB #######################################################################
        ############################################################################################
        self.datasets_table = Table(
            self,
            "datasets-table",
            deploy_env=deploy_env,
            application_layer="application-db",
        )

        self.datasets_table.add_global_secondary_index(
            index_name=ResourceName.DATASETS_TABLE_TITLE_INDEX_NAME.value,
            partition_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(name="title", type=aws_dynamodb.AttributeType.STRING),
        )
        self.datasets_table.add_global_secondary_index(
            index_name=ResourceName.DATASETS_TABLE_OWNING_GROUP_INDEX_NAME.value,
            partition_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(
                name="owning_group", type=aws_dynamodb.AttributeType.STRING
            ),
        )
        self.datasets_table_name_parameter = aws_ssm.StringParameter(
            self,
            "Datasets Table Name Parameter",
            description=f"Datasets Table name for {deploy_env}",
            parameter_name=ParameterName.DATASETS_TABLE_NAME.value,
            string_value=self.datasets_table.table_name,
        )
