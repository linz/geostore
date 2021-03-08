"""
Data Lake AWS resources definitions.
"""
from typing import Any

from aws_cdk import aws_dynamodb, aws_iam, aws_lambda, aws_ssm, aws_stepfunctions, core
from aws_cdk.aws_iam import PolicyStatement
from aws_cdk.core import Duration, Tags


class APIStack(core.Stack):
    """Data Lake stack definition."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        scope: core.Construct,
        stack_id: str,
        datasets_table: aws_dynamodb.Table,
        users_role: aws_iam.Role,
        deploy_env: str,
        state_machine: aws_stepfunctions.StateMachine,
        state_machine_parameter: aws_ssm.StringParameter,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, stack_id, **kwargs)

        ############################################################################################
        # ### API ENDPOINTS ########################################################################
        ############################################################################################

        endpoints = ("datasets", "dataset_versions", "import_status")

        for endpoint in endpoints:
            endpoint_function = aws_lambda.Function(
                self,
                f"{deploy_env}-{endpoint}-endpoint-function",
                function_name=f"{deploy_env}-{endpoint}-endpoint",
                handler=f"backend.{endpoint}.entrypoint.lambda_handler",
                runtime=aws_lambda.Runtime.PYTHON_3_8,
                timeout=Duration.seconds(60),
                code=aws_lambda.Code.from_asset(
                    path=".",
                    bundling=core.BundlingOptions(
                        # pylint:disable=no-member
                        image=aws_lambda.Runtime.PYTHON_3_8.bundling_docker_image,
                        command=["backend/bundle.bash", f"{endpoint}"],
                    ),
                ),
            )

            endpoint_function.add_environment("DEPLOY_ENV", deploy_env)
            endpoint_function.grant_invoke(users_role)  # type: ignore[arg-type]

            datasets_table.grant_read_write_data(endpoint_function)
            datasets_table.grant(
                endpoint_function, "dynamodb:DescribeTable"
            )  # required by pynamodb

            Tags.of(endpoint_function).add("ApplicationLayer", "api")

            # dataset_versions specific permissions
            if endpoint == "dataset_versions":
                state_machine_parameter.grant_read(endpoint_function)
                state_machine.grant_start_execution(endpoint_function)

            if endpoint == "import_status":
                state_machine.grant_read(endpoint_function)
                assert endpoint_function.role is not None
                endpoint_function.role.add_to_policy(
                    PolicyStatement(
                        resources=["*"],
                        actions=["s3:DescribeJob"],
                    ),
                )
