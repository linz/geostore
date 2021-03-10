from aws_cdk import aws_iam, aws_lambda, core
from aws_cdk.core import Duration, Tags


class LambdaEndpoint(core.Construct):
    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        *,
        deploy_env: str,
        users_role: aws_iam.Role,
    ):
        super().__init__(scope, construct_id)

        self.lambda_function = aws_lambda.Function(
            self,
            f"{deploy_env}-{construct_id}-endpoint-function",
            function_name=f"{deploy_env}-{construct_id}-endpoint",
            handler=f"backend.{construct_id}.entrypoint.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            timeout=Duration.seconds(60),
            code=aws_lambda.Code.from_asset(
                path=".",
                bundling=core.BundlingOptions(
                    # pylint:disable=no-member
                    image=aws_lambda.Runtime.PYTHON_3_8.bundling_docker_image,
                    command=["backend/bundle.bash", f"{construct_id}"],
                ),
            ),
        )

        self.lambda_function.add_environment("DEPLOY_ENV", deploy_env)
        self.lambda_function.grant_invoke(users_role)  # type: ignore[arg-type]

        Tags.of(self.lambda_function).add("ApplicationLayer", "api")
