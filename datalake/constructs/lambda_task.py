from typing import Callable, Iterable, Mapping, Optional

from aws_cdk import aws_stepfunctions_tasks, core
from aws_cdk.aws_iam import Grant, IGrantable

from .bundled_lambda_function import BundledLambdaFunction


class LambdaTask(core.Construct):
    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        *,
        directory: str,
        result_path: str,
        application_layer: str,
        permission_functions: Optional[Iterable[Callable[[IGrantable], Grant]]] = None,
        extra_environment: Optional[Mapping[str, str]] = None,
    ):
        super().__init__(scope, construct_id)

        lambda_function = BundledLambdaFunction(
            self,
            f"{construct_id}_bundled_lambda_function",
            directory=directory,
            application_layer=application_layer,
            extra_environment=extra_environment,
        )

        if permission_functions is not None:
            for permission_function in permission_functions:
                permission_function(lambda_function)

        self.lambda_invoke = aws_stepfunctions_tasks.LambdaInvoke(
            scope,
            f"{construct_id}_lambda_invoke",
            lambda_function=lambda_function,
            result_path=result_path,
            payload_response_only=True,
        )
