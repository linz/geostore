"""
Geostore AWS resources definitions.
"""
from aws_cdk import aws_dynamodb, aws_s3, aws_ssm
from aws_cdk.core import Construct, Tags

from geostore.datasets_model import DatasetsTitleIdx
from geostore.parameter_store import ParameterName
from geostore.resources import Resource
from geostore.validation_results_model import ValidationOutcomeIdx

from .removal_policy import REMOVAL_POLICY
from .table import Table
from .version import GIT_BRANCH, GIT_COMMIT, GIT_TAG


class Storage(Construct):
    def __init__(self, scope: Construct, stack_id: str, *, env_name: str) -> None:
        super().__init__(scope, stack_id)

        ############################################################################################
        # ### DEPLOYMENT VERSION ###################################################################
        ############################################################################################

        aws_ssm.StringParameter(
            self,
            "git-branch",
            parameter_name=f"/{env_name}/git_branch",
            string_value=GIT_BRANCH,
            description="Deployment git branch",
        )

        aws_ssm.StringParameter(
            self,
            "git-commit",
            parameter_name=f"/{env_name}/git_commit",
            string_value=GIT_COMMIT,
            description="Deployment git commit",
        )

        aws_ssm.StringParameter(
            self,
            "git-tag",
            parameter_name=f"/{env_name}/version",
            string_value=GIT_TAG,
            description="Deployment version",
        )

        ############################################################################################
        # ### STORAGE S3 BUCKET ####################################################################
        ############################################################################################
        self.storage_bucket = aws_s3.Bucket(
            self,
            "storage-bucket",
            bucket_name=Resource.STORAGE_BUCKET_NAME.resource_name,
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
            f"{env_name}-datasets",
            env_name=env_name,
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
            f"{env_name}-validation-results",
            env_name=env_name,
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

        Tags.of(self).add("ApplicationLayer", "storage")
