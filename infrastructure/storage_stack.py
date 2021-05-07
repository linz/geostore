"""
Data Lake AWS resources definitions.
"""
from typing import Any

from aws_cdk import aws_dynamodb, aws_s3, aws_ssm
from aws_cdk.core import Construct, NestedStack, Tags

from backend.datasets_model import DatasetsTitleIdx
from backend.parameter_store import ParameterName
from backend.resources import ResourceName
from backend.validation_results_model import ValidationOutcomeIdx
from backend.version import GIT_BRANCH, GIT_COMMIT, GIT_TAG

from .constructs.table import Table
from .removal_policy import REMOVAL_POLICY


class StorageStack(NestedStack):
    def __init__(self, scope: Construct, stack_id: str, *, deploy_env: str, **kwargs: Any) -> None:
        super().__init__(scope, stack_id, **kwargs)

        ############################################################################################
        # ### DEPLOYMENT VERSION ###################################################################
        ############################################################################################

        aws_ssm.StringParameter(
            self,
            "git-branch",
            parameter_name=f"/{deploy_env}/git_branch",
            string_value=GIT_BRANCH,
            description="Deployment git branch",
        )

        aws_ssm.StringParameter(
            self,
            "git-commit",
            parameter_name=f"/{deploy_env}/git_commit",
            string_value=GIT_COMMIT,
            description="Deployment git commit",
        )

        aws_ssm.StringParameter(
            self,
            "git-tag",
            parameter_name=f"/{deploy_env}/version",
            string_value=GIT_TAG,
            description="Deployment version",
        )

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
            removal_policy=REMOVAL_POLICY,
        )

        ############################################################################################
        # ### APPLICATION DB #######################################################################
        ############################################################################################
        self.datasets_table = Table(
            self,
            f"{deploy_env}-datasets",
            deploy_env=deploy_env,
            parameter_name=ParameterName.STORAGE_DATASETS_TABLE_NAME,
        )

        self.datasets_table.add_global_secondary_index(
            index_name=DatasetsTitleIdx.Meta.index_name,
            partition_key=aws_dynamodb.Attribute(
                name="title", type=aws_dynamodb.AttributeType.STRING
            ),
        )

        self.validation_results_table = Table(
            self,
            f"{deploy_env}-validation-results",
            deploy_env=deploy_env,
            parameter_name=ParameterName.STORAGE_VALIDATION_RESULTS_TABLE_NAME,
            sort_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
        )

        self.validation_results_table.add_global_secondary_index(
            index_name=ValidationOutcomeIdx.Meta.index_name,
            partition_key=aws_dynamodb.Attribute(
                name=ValidationOutcomeIdx.pk.attr_name, type=aws_dynamodb.AttributeType.STRING
            ),
            sort_key=aws_dynamodb.Attribute(
                name=ValidationOutcomeIdx.result.attr_name, type=aws_dynamodb.AttributeType.STRING
            ),
        )

        Tags.of(self).add("ApplicationLayer", "storage")  # type: ignore[arg-type]
