from aws_cdk import aws_lambda, aws_lambda_python
from aws_cdk.core import Construct, Duration

from ..runtime import PYTHON_RUNTIME
from .backend import BACKEND_DIRECTORY
from .bundled_code import bundled_code


class LambdaEndpoint(aws_lambda.Function):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        deploy_env: str,
        package_name: str,
        botocore_lambda_layer: aws_lambda_python.PythonLayerVersion,
    ):
        super().__init__(
            scope,
            construct_id,
            code=bundled_code(package_name),
            environment=({"DEPLOY_ENV": deploy_env}),
            function_name=f"{deploy_env}-{construct_id}",
            handler=f"{BACKEND_DIRECTORY}.{package_name}.entrypoint.lambda_handler",
            layers=[botocore_lambda_layer],  # type: ignore[list-item]
            runtime=PYTHON_RUNTIME,
            timeout=Duration.seconds(60),
        )
