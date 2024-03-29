from typing import Mapping, Optional

from aws_cdk import Duration, aws_lambda, aws_lambda_python_alpha
from constructs import Construct

from .backend import BACKEND_DIRECTORY
from .bundled_code import bundled_code
from .common import LOG_LEVEL
from .lambda_config import (
    DEFAULT_LAMBDA_MAX_MEMORY_MEBIBYTES,
    DEFAULT_LAMBDA_TIMEOUT,
    PYTHON_RUNTIME,
)


class BundledLambdaFunction(aws_lambda.Function):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        lambda_directory: str,
        extra_environment: Optional[Mapping[str, str]],
        botocore_lambda_layer: aws_lambda_python_alpha.PythonLayerVersion,
        timeout: Duration = DEFAULT_LAMBDA_TIMEOUT,
        reserved_concurrent_executions: Optional[int] = None,
    ):
        environment = {"LOGLEVEL": LOG_LEVEL}
        if extra_environment is not None:
            environment.update(extra_environment)

        super().__init__(
            scope,
            construct_id,
            code=bundled_code(lambda_directory),
            handler=f"{BACKEND_DIRECTORY}.{lambda_directory}.task.lambda_handler",
            runtime=PYTHON_RUNTIME,
            environment=environment,
            layers=[botocore_lambda_layer],
            timeout=timeout,
            memory_size=DEFAULT_LAMBDA_MAX_MEMORY_MEBIBYTES,
            reserved_concurrent_executions=reserved_concurrent_executions,
        )
