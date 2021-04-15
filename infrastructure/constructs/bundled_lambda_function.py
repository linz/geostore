from typing import Mapping, Optional

from aws_cdk import aws_lambda
from aws_cdk.core import Construct, Duration, Tags

from ..common import LOG_LEVEL, PROJECT_DIRECTORY


class BundledLambdaFunction(aws_lambda.Function):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        directory: str,
        application_layer: str,
        extra_environment: Optional[Mapping[str, str]] = None,
    ):
        lambda_code = aws_lambda.Code.from_asset_image(
            directory=PROJECT_DIRECTORY,
            cmd=["-m", "src.task.task", "lambda_handler"],
            build_args={"task": directory},
            file="backend/Dockerfile",
        )

        environment = {"LOGLEVEL": LOG_LEVEL}
        if extra_environment is not None:
            environment.update(extra_environment)

        super().__init__(
            scope,
            construct_id,
            code=lambda_code,
            handler=aws_lambda.Handler.FROM_IMAGE,
            runtime=aws_lambda.Runtime.FROM_IMAGE,
            environment=environment,
            timeout=Duration.seconds(60),
        )

        Tags.of(self).add("ApplicationLayer", application_layer)
