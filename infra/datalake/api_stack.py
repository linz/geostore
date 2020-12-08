"""
Data Lake AWS resources definitions.
"""
from aws_cdk import aws_lambda, core
from aws_cdk.core import Tags


class APIStack(core.Stack):
    """Data Lake stack definition."""

    def __init__(self, scope: core.Construct, stack_id: str, datasets_table, **kwargs) -> None:
        super().__init__(scope, stack_id, **kwargs)

        ############################################################################################
        # ### API ENDPOINTS ########################################################################
        ############################################################################################

        endpoints = ("datasets",)

        for endpoint in endpoints:
            endpoint_function = aws_lambda.Function(
                self,
                f"{endpoint}-endpoint-function",
                function_name=f"{endpoint}-endpoint",
                handler=f"endpoints.{endpoint}.entrypoint.lambda_handler",
                runtime=aws_lambda.Runtime.PYTHON_3_6,
                code=aws_lambda.Code.from_asset(
                    path="..",
                    bundling=core.BundlingOptions(
                        # pylint:disable=no-member
                        image=aws_lambda.Runtime.PYTHON_3_6.bundling_docker_image,
                        command=["backend/bundle.bash", f"endpoints/{endpoint}"],
                    ),
                ),
            )

            datasets_table.grant_read_write_data(endpoint_function)
            datasets_table.grant(
                endpoint_function, "dynamodb:DescribeTable"
            )  # required by pynamodb

            Tags.of(endpoint_function).add("ApplicationLayer", "api")
