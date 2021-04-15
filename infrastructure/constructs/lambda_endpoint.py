from aws_cdk import aws_iam, aws_lambda
from aws_cdk.core import Construct, Duration, Tags

from ..common import PROJECT_DIRECTORY


class LambdaEndpoint(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        deploy_env: str,
        users_role: aws_iam.Role,
        package_name: str,
    ):
        super().__init__(scope, construct_id)

        code = aws_lambda.Code.from_asset_image(
            directory=PROJECT_DIRECTORY,
            cmd=["-m", "src.task.entrypoint", "lambda_handler"],
            build_args={"task": package_name},
            file="backend/Dockerfile",
        )
        self.lambda_function = aws_lambda.Function(
            self,
            f"{deploy_env}-{construct_id}-function",
            function_name=f"{deploy_env}-{construct_id}",
            handler=aws_lambda.Handler.FROM_IMAGE,
            runtime=aws_lambda.Runtime.FROM_IMAGE,
            timeout=Duration.seconds(60),
            code=code,
        )

        self.lambda_function.add_environment("DEPLOY_ENV", deploy_env)
        self.lambda_function.grant_invoke(users_role)  # type: ignore[arg-type]

        Tags.of(self.lambda_function).add("ApplicationLayer", "api")
