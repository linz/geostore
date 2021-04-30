from typing import Any

import constructs
from aws_cdk import aws_lambda_python
from aws_cdk.core import NestedStack

from .runtime import PYTHON_RUNTIME


class LambdaLayersStack(NestedStack):
    def __init__(
        self,
        scope: constructs.Construct,
        stack_id: str,
        *,
        deploy_env: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, stack_id, **kwargs)

        self.botocore = aws_lambda_python.PythonLayerVersion(
            self,
            f"{deploy_env}-botocore-lambda-layer",
            entry="infrastructure/lambda_layers/botocore",
            compatible_runtimes=[PYTHON_RUNTIME],
            description="botocore library",
        )
