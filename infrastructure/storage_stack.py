"""
Data Lake AWS resources definitions.
"""
from typing import Any

from aws_cdk import aws_dynamodb, aws_s3, aws_ssm
from aws_cdk.core import Construct, RemovalPolicy, Stack, Tags

from backend.datasets_model import DatasetsOwningGroupIdx, DatasetsTitleIdx
from backend.environment import ENV
from backend.parameter_store import ParameterName

from .constructs.table import Table


class StorageStack(Stack):
    def __init__(self, scope: Construct, stack_id: str, deploy_env: str, **kwargs: Any) -> None:
        super().__init__(scope, stack_id, **kwargs)

        # set resources depending on deployment type
        if deploy_env == "prod":
            resource_removal_policy = RemovalPolicy.RETAIN
        else:
            resource_removal_policy = RemovalPolicy.DESTROY

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

        self.storage_bucket_parameter = aws_ssm.StringParameter(
            self,
            "storage bucket name",
            description=f"Storage bucket name for {deploy_env}",
            parameter_name=ParameterName.STORAGE_BUCKET_NAME.value,
            string_value=self.storage_bucket.bucket_name,
        )

        ############################################################################################
        # ### APPLICATION DB #######################################################################
        ############################################################################################
        self.datasets_table = Table(
            self,
            f"{ENV}-datasets",
            deploy_env=deploy_env,
            application_layer="application-db",
        )

        self.datasets_table.add_global_secondary_index(
            index_name=DatasetsTitleIdx.Meta.index_name,
            partition_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(name="title", type=aws_dynamodb.AttributeType.STRING),
        )
        self.datasets_table.add_global_secondary_index(
            index_name=DatasetsOwningGroupIdx.Meta.index_name,
            partition_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(
                name="owning_group", type=aws_dynamodb.AttributeType.STRING
            ),
        )

        self.datasets_table_name_parameter = aws_ssm.StringParameter(
            self,
            "datasets table name",
            description=f"Datasets table name for {deploy_env}",
            string_value=self.datasets_table.table_name,
            parameter_name=ParameterName.DATASETS_TABLE_NAME.value,
        )
