from typing import Mapping, Optional

from aws_cdk import aws_lambda, core
from aws_cdk.core import Duration

from ..common import LOG_LEVEL


class BundledLambdaFunction(aws_lambda.Function):
    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        *,
        directory: str,
        application_layer: str,
        extra_environment: Optional[Mapping[str, str]] = None,
    ):
        bundling_options = core.BundlingOptions(
            # pylint:disable=no-member
            image=aws_lambda.Runtime.PYTHON_3_8.bundling_docker_image,
            command=["backend/bundle.bash", directory],
        )
        lambda_code = aws_lambda.Code.from_asset(path=".", bundling=bundling_options)

        environment = {"LOGLEVEL": LOG_LEVEL}
        if extra_environment is not None:
            environment.update(extra_environment)

        super().__init__(
            scope,
            construct_id,
            code=lambda_code,
            handler=f"backend.{directory}.task.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            environment=environment,
            timeout=Duration.seconds(60),
        )

        core.Tags.of(self).add("ApplicationLayer", application_layer)
