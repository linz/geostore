from os import environ

import constructs
from aws_cdk import Environment, Stack, aws_iam

from geostore.environment import environment_name

from .constructs.api import API
from .constructs.lambda_layers import LambdaLayers
from .constructs.lds import LDS
from .constructs.notify import Notify
from .constructs.opentopo import OpenTopography
from .constructs.processing import Processing
from .constructs.staging import Staging
from .constructs.storage import Storage


class Application(Stack):
    def __init__(self, scope: constructs.Construct, stack_id: str) -> None:
        environment = Environment(
            account=environ["CDK_DEFAULT_ACCOUNT"], region=environ["CDK_DEFAULT_REGION"]
        )

        super().__init__(scope, stack_id, env=environment)

        env_name = environment_name()

        principal: aws_iam.PrincipalBase
        if saml_provider_arn := environ.get("GEOSTORE_SAML_IDENTITY_PROVIDER_ARN"):
            principal = aws_iam.FederatedPrincipal(
                federated=saml_provider_arn,
                assume_role_action="sts:AssumeRoleWithSAML",
                conditions={"StringEquals": {"SAML:aud": "https://signin.aws.amazon.com/saml"}},
            )
        else:
            open_id_connect_provider_arn = (
                "arn:aws:iam::"
                + aws_iam.AccountRootPrincipal().account_id
                + ":oidc-provider/token.actions.githubusercontent.com"
            )

            principal = aws_iam.WebIdentityPrincipal(
                identity_provider=open_id_connect_provider_arn,
                conditions={
                    "StringLike": {
                        "token.actions.githubusercontent.com:aud": ["sts.amazonaws.com"],
                        "token.actions.githubusercontent.com:sub": ["repo:linz/geostore:*"],
                    }
                },
            )

        storage = Storage(self, "storage", env_name=env_name)

        lambda_layers = LambdaLayers(self, "lambda-layers", env_name=env_name)

        processing = Processing(
            self,
            "processing",
            botocore_lambda_layer=lambda_layers.botocore,
            env_name=env_name,
            principal=principal,
            s3_role_arn_parameter=storage.s3_role_arn_parameter,
            storage_bucket=storage.storage_bucket,
            validation_results_table=storage.validation_results_table,
            datasets_table=storage.datasets_table,
        )
        Staging(self, "staging", users_role=processing.staging_users_role)

        API(
            self,
            "api",
            botocore_lambda_layer=lambda_layers.botocore,
            datasets_table=storage.datasets_table,
            env_name=env_name,
            state_machine=processing.state_machine,
            state_machine_parameter=processing.state_machine_parameter,
            sqs_queue=processing.message_queue,
            sqs_queue_parameter=processing.message_queue_name_parameter,
            storage_bucket=storage.storage_bucket,
            validation_results_table=storage.validation_results_table,
        )

        Notify(
            self,
            "notify",
            botocore_lambda_layer=lambda_layers.botocore,
            env_name=env_name,
            state_machine=processing.state_machine,
            validation_results_table=storage.validation_results_table,
        )

        if self.node.try_get_context("enableLDSAccess"):
            LDS(self, "lds", env_name=env_name, storage_bucket=storage.storage_bucket)

        if self.node.try_get_context("enableOpenTopographyAccess"):
            OpenTopography(
                self, "opentopography", env_name=env_name, storage_bucket=storage.storage_bucket
            )
