import constructs
from aws_cdk import aws_lambda_python_alpha
from constructs import Construct

from .lambda_config import PYTHON_RUNTIME


class LambdaLayers(Construct):
    def __init__(self, scope: constructs.Construct, stack_id: str, *, env_name: str) -> None:
        super().__init__(scope, stack_id)

        self.botocore = aws_lambda_python_alpha.PythonLayerVersion(
            self,
            f"{env_name}-botocore-lambda-layer",
            entry="infrastructure/constructs/lambda_layers/botocore",
            bundling=aws_lambda_python_alpha.BundlingOptions(
                # See https://github.com/aws/aws-cdk/issues/21867#issuecomment-1234233411
                environment={"POETRY_VIRTUALENVS_IN_PROJECT": "true"}
            ),
            compatible_runtimes=[PYTHON_RUNTIME],
            description="botocore library",
        )
