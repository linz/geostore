from typing import Mapping, Optional

from aws_cdk import aws_stepfunctions_tasks
from aws_cdk.core import Construct

from .bundled_lambda_function import BundledLambdaFunction


class LambdaTask(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        directory: str,
        result_path: str,
        application_layer: str,
        extra_environment: Optional[Mapping[str, str]] = None,
    ):
        super().__init__(scope, construct_id)

        self.lambda_function = BundledLambdaFunction(
            self,
            f"{construct_id}_bundled_lambda_function",
            directory=directory,
            application_layer=application_layer,
            extra_environment=extra_environment,
        )

        self.lambda_invoke = aws_stepfunctions_tasks.LambdaInvoke(
            scope,
            f"{construct_id}_lambda_invoke",
            lambda_function=self.lambda_function,
            result_path=result_path,
            payload_response_only=True,
        )
