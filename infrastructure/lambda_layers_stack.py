from typing import Any

from aws_cdk import aws_lambda_python
from aws_cdk.core import Construct, NestedStack

from backend.environment import ENV
from infrastructure.runtime import PYTHON_RUNTIME


class LambdaLayersStack(NestedStack):
    def __init__(
        self,
        scope: Construct,
        stack_id: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, stack_id, **kwargs)

        self.botocore = aws_lambda_python.PythonLayerVersion(
            self,
            f"{ENV}-botocore-lambda-layer",
            entry="infrastructure/lambda_layers/botocore",
            compatible_runtimes=[PYTHON_RUNTIME],
            description="botocore library",
        )
