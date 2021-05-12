from aws_cdk import aws_iam, aws_lambda, aws_lambda_python
from aws_cdk.core import Construct, Duration

from .backend import BACKEND_DIRECTORY
from .bundled_code import bundled_code
from .runtime import PYTHON_RUNTIME


class LambdaEndpoint(aws_lambda.Function):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        deploy_env: str,
        users_role: aws_iam.Role,
        package_name: str,
        botocore_lambda_layer: aws_lambda_python.PythonLayerVersion,
    ):
        super().__init__(
            scope,
            f"{deploy_env}-{construct_id}-function",
            function_name=f"{deploy_env}-{construct_id}",
            handler=f"{BACKEND_DIRECTORY}.{package_name}.entrypoint.lambda_handler",
            runtime=PYTHON_RUNTIME,
            timeout=Duration.seconds(60),
            code=bundled_code(package_name),
            layers=[botocore_lambda_layer],  # type: ignore[list-item]
        )

        self.add_environment("DEPLOY_ENV", deploy_env)
        self.grant_invoke(users_role)  # type: ignore[arg-type]
