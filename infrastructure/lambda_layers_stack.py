from typing import Any, Optional

import constructs
from aws_cdk import aws_lambda, aws_lambda_python
from aws_cdk.core import Stack

from backend.environment import ENV


class LambdaLayersStack(Stack):
    def __init__(
        self,
        scope: Optional[constructs.Construct] = None,
        stack_id: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, stack_id, **kwargs)

        self.botocore = aws_lambda_python.PythonLayerVersion(
            self,
            f"{ENV}-botocore-lambda-layer",
            entry="infrastructure/lambda_layers/botocore",
            compatible_runtimes=[aws_lambda.Runtime.PYTHON_3_8],
            description="botocore library",
        )
