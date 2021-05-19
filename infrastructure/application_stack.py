from os import environ

import constructs
from aws_cdk.core import Environment, Stack

from backend.environment import environment_name

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

        env_name = environment_name()
        storage = Storage(self, "storage", env_name=env_name)

        Staging(self, "staging")

        lambda_layers = LambdaLayers(self, "lambda-layers", env_name=env_name)

        processing = Processing(
            self,
            "processing",
            botocore_lambda_layer=lambda_layers.botocore,
            datasets_table=storage.datasets_table,
            env_name=env_name,
            storage_bucket=storage.storage_bucket,
            validation_results_table=storage.validation_results_table,
        )

        API(
            self,
            "api",
            botocore_lambda_layer=lambda_layers.botocore,
            datasets_table=storage.datasets_table,
            env_name=env_name,
            state_machine=processing.state_machine,
            state_machine_parameter=processing.state_machine_parameter,
            storage_bucket=storage.storage_bucket,
            validation_results_table=storage.validation_results_table,
        )

        if self.node.try_get_context("enableLDSAccess"):
            LDS(self, "lds", env_name=env_name, storage_bucket=storage.storage_bucket)
