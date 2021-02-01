"""
Data Lake AWS resources definitions.
"""
from typing import Any

from aws_cdk import aws_dynamodb, aws_s3, core
from aws_cdk.core import Tags

from .backend.endpoints.datasets.model import (
    DATASETS_OWNING_GROUP_INDEX_NAME,
    DATASETS_TITLE_INDEX_NAME,
)
from .backend.endpoints.utils import ResourceName
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
            bucket_name=ResourceName.STORAGE_BUCKET_NAME.value,
            access_control=aws_s3.BucketAccessControl.PRIVATE,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            removal_policy=resource_removal_policy,
        )
        Tags.of(self.storage_bucket).add("ApplicationLayer", "storage")

        ############################################################################################
        # ### APPLICATION DB #######################################################################
        ############################################################################################
        self.datasets_table = Table(
            self,
            ResourceName.DATASETS_TABLE_NAME.value,
            deploy_env=deploy_env,
            application_layer="application-db",
        )

        self.datasets_table.add_global_secondary_index(
            index_name=DATASETS_TITLE_INDEX_NAME,
            partition_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(name="title", type=aws_dynamodb.AttributeType.STRING),
        )
        self.datasets_table.add_global_secondary_index(
            index_name=DATASETS_OWNING_GROUP_INDEX_NAME,
            partition_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(
                name="owning_group", type=aws_dynamodb.AttributeType.STRING
            ),
        )
