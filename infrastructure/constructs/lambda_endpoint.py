from aws_cdk import aws_iam, aws_lambda, aws_lambda_python_alpha
from constructs import Construct

from geostore.environment import ENV_NAME_VARIABLE_NAME

from .backend import BACKEND_DIRECTORY
from .bundled_code import bundled_code
from .lambda_config import (
    DEFAULT_LAMBDA_MAX_MEMORY_MEBIBYTES,
    DEFAULT_LAMBDA_TIMEOUT,
    PYTHON_RUNTIME,
)


class LambdaEndpoint(aws_lambda.Function):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        users_role: aws_iam.Role,
        package_name: str,
        botocore_lambda_layer: aws_lambda_python_alpha.PythonLayerVersion,
    ):
        super().__init__(
            scope,
            f"{construct_id}-function",
            function_name=construct_id,
            handler=f"{BACKEND_DIRECTORY}.{package_name}.entrypoint.lambda_handler",
            runtime=PYTHON_RUNTIME,
            timeout=DEFAULT_LAMBDA_TIMEOUT,
            code=bundled_code(package_name),
            layers=[botocore_lambda_layer],
            memory_size=DEFAULT_LAMBDA_MAX_MEMORY_MEBIBYTES,
        )

        self.add_environment(ENV_NAME_VARIABLE_NAME, env_name)
        self.grant_invoke(users_role)
