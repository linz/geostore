from typing import Mapping, Optional

from aws_cdk import aws_stepfunctions_tasks
from aws_cdk.aws_stepfunctions import JsonPath
from constructs import Construct

from .bundled_lambda_function import BundledLambdaFunction


class LambdaTask(aws_stepfunctions_tasks.LambdaInvoke):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        directory: str,
        result_path: Optional[str] = JsonPath.DISCARD,
        extra_environment: Optional[Mapping[str, str]] = None,
    ):
        self.lambda_function = BundledLambdaFunction(
            scope,
            f"{construct_id}Function",
            directory=directory,
            extra_environment=extra_environment,
        )

        super().__init__(
            scope,
            construct_id,
            lambda_function=self.lambda_function,
            result_path=result_path,
            payload_response_only=True,
        )
