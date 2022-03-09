from aws_cdk import aws_iam, aws_lambda, aws_lambda_python
from aws_cdk.core import Construct

from geostore.environment import ENV_NAME_VARIABLE_NAME

from .backend import BACKEND_DIRECTORY
from .bundled_code import bundled_code
from .lambda_config import DEFAULT_LAMBDA_MAX_MEMORY_MEGABYTES, LAMBDA_TIMEOUT, PYTHON_RUNTIME


class LambdaEndpoint(aws_lambda.Function):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        users_role: aws_iam.Role,
        package_name: str,
        botocore_lambda_layer: aws_lambda_python.PythonLayerVersion,
    ):
        super().__init__(
            scope,
            f"{env_name}-{construct_id}-function",
            function_name=f"{env_name}-{construct_id}",
            handler=f"{BACKEND_DIRECTORY}.{package_name}.entrypoint.lambda_handler",
            runtime=PYTHON_RUNTIME,
            timeout=LAMBDA_TIMEOUT,
            code=bundled_code(package_name),
            layers=[botocore_lambda_layer],  # type: ignore[list-item]
            memory_size=DEFAULT_LAMBDA_MAX_MEMORY_MEGABYTES,
        )

        self.add_environment(ENV_NAME_VARIABLE_NAME, env_name)
        self.grant_invoke(users_role)  # type: ignore[arg-type]
