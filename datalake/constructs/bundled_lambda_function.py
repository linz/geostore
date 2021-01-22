from aws_cdk import aws_lambda, core


class BundledLambdaFunction(aws_lambda.Function):
    def __init__(
        self, scope: core.Construct, construct_id: str, *, directory: str, application_layer: str
    ):
        bundling_options = core.BundlingOptions(
            # pylint:disable=no-member
            image=aws_lambda.Runtime.PYTHON_3_8.bundling_docker_image,
            command=["datalake/backend/bundle.bash", f"processing/{directory}"],
        )
        lambda_code = aws_lambda.Code.from_asset(path=".", bundling=bundling_options)

        super().__init__(
            scope,
            construct_id,
            handler=f"processing.{directory}.task.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            code=lambda_code,
        )

        core.Tags.of(self).add("ApplicationLayer", application_layer)
