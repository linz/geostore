from os import environ

import constructs
from aws_cdk.core import Environment, Stack

from backend.environment import ENV

from .constructs.api import API
from .constructs.lambda_layers import LambdaLayers
from .constructs.lds import LDS
from .constructs.processing import Processing
from .constructs.staging import Staging
from .constructs.storage import Storage


class Application(Stack):
    def __init__(self, scope: constructs.Construct, stack_id: str) -> None:
        environment = Environment(
            account=environ["CDK_DEFAULT_ACCOUNT"], region=environ["CDK_DEFAULT_REGION"]
        )

        super().__init__(scope, stack_id, env=environment)

        storage = Storage(self, "storage", deploy_env=ENV)

        Staging(self, "staging")

        lambda_layers = LambdaLayers(self, "lambda-layers", deploy_env=ENV)

        processing = Processing(
            self,
            "processing",
            botocore_lambda_layer=lambda_layers.botocore,
            datasets_table=storage.datasets_table,
            deploy_env=ENV,
            storage_bucket=storage.storage_bucket,
            validation_results_table=storage.validation_results_table,
        )

        API(
            self,
            "api",
            botocore_lambda_layer=lambda_layers.botocore,
            datasets_table=storage.datasets_table,
            deploy_env=ENV,
            state_machine=processing.state_machine,
            state_machine_parameter=processing.state_machine_parameter,
            storage_bucket=storage.storage_bucket,
            validation_results_table=storage.validation_results_table,
        )

        if self.node.try_get_context("enableLDSAccess"):
            LDS(self, "lds", deploy_env=ENV, storage_bucket=storage.storage_bucket)
