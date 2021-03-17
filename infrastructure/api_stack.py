"""
Data Lake AWS resources definitions.
"""
from typing import Any

from aws_cdk import aws_dynamodb, aws_iam, aws_ssm, aws_stepfunctions
from aws_cdk.core import Construct, Stack

from infrastructure.constructs.lambda_endpoint import LambdaEndpoint


class APIStack(Stack):
    """Data Lake stack definition."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        scope: Construct,
        stack_id: str,
        datasets_table: aws_dynamodb.Table,
        validation_results_table: aws_dynamodb.Table,
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

        datasets_endpoint_lambda = LambdaEndpoint(
            self,
            "datasets",
            deploy_env=deploy_env,
            users_role=users_role,
        ).lambda_function

        dataset_versions_endpoint_lambda = LambdaEndpoint(
            self,
            "dataset_versions",
            deploy_env=deploy_env,
            users_role=users_role,
        ).lambda_function

        state_machine_parameter.grant_read(dataset_versions_endpoint_lambda)
        state_machine.grant_start_execution(dataset_versions_endpoint_lambda)

        for function in [datasets_endpoint_lambda, dataset_versions_endpoint_lambda]:
            datasets_table.grant_read_write_data(function)
            datasets_table.grant(function, "dynamodb:DescribeTable")  # required by pynamodb

        import_status_endpoint_lambda = LambdaEndpoint(
            self,
            "import_status",
            deploy_env=deploy_env,
            users_role=users_role,
        ).lambda_function

        validation_results_table.grant_read_data(import_status_endpoint_lambda)
        validation_results_table.grant(
            import_status_endpoint_lambda, "dynamodb:DescribeTable"
        )  # required by pynamodb

        state_machine.grant_read(import_status_endpoint_lambda)
        assert import_status_endpoint_lambda.role is not None
        import_status_endpoint_lambda.role.add_to_policy(
            aws_iam.PolicyStatement(
                resources=["*"],
                actions=["s3:DescribeJob"],
            ),
        )
