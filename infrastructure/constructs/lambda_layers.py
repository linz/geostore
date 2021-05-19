import constructs
from aws_cdk import aws_lambda_python
from aws_cdk.core import Construct

from .runtime import PYTHON_RUNTIME


class LambdaLayers(Construct):
    def __init__(self, scope: constructs.Construct, stack_id: str, *, env_name: str) -> None:
        super().__init__(scope, stack_id)

        self.botocore = aws_lambda_python.PythonLayerVersion(
            self,
            f"{env_name}-botocore-lambda-layer",
            entry="infrastructure/constructs/lambda_layers/botocore",
            compatible_runtimes=[PYTHON_RUNTIME],
            description="botocore library",
        )
