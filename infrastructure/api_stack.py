"""
Data Lake AWS resources definitions.
"""
from aws_cdk import aws_iam, aws_lambda_python, aws_s3, aws_ssm, aws_stepfunctions
from aws_cdk.core import Construct, NestedStack, Tags

from .common import grant_parameter_read_access
from .constructs.lambda_endpoint import LambdaEndpoint
from .constructs.table import Table


class APIStack(NestedStack):
    """Data Lake stack definition."""

    def __init__(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        scope: Construct,
        stack_id: str,
        *,
        api_users_role: aws_iam.Role,
        botocore_lambda_layer: aws_lambda_python.PythonLayerVersion,
        datasets_table: Table,
        deploy_env: str,
        s3_users_role: aws_iam.Role,
        state_machine: aws_stepfunctions.StateMachine,
        state_machine_parameter: aws_ssm.StringParameter,
        storage_bucket: aws_s3.Bucket,
        validation_results_table: Table,
    ) -> None:
        super().__init__(scope, stack_id)

        ############################################################################################
        # ### API ENDPOINTS ########################################################################
        ############################################################################################

        datasets_endpoint_lambda = LambdaEndpoint(
            self,
            "datasets",
            package_name="datasets",
            deploy_env=deploy_env,
            botocore_lambda_layer=botocore_lambda_layer,
        )

        datasets_endpoint_lambda.grant_invoke(api_users_role)  # type: ignore[arg-type]

        dataset_versions_endpoint_lambda = LambdaEndpoint(
            self,
            "dataset-versions",
            package_name="dataset_versions",
            deploy_env=deploy_env,
            botocore_lambda_layer=botocore_lambda_layer,
        )

        dataset_versions_endpoint_lambda.grant_invoke(api_users_role)  # type: ignore[arg-type]

        state_machine.grant_start_execution(dataset_versions_endpoint_lambda)

        storage_bucket.grant_read(datasets_endpoint_lambda)

        for function in [datasets_endpoint_lambda, dataset_versions_endpoint_lambda]:
            datasets_table.grant_read_write_data(function)
            datasets_table.grant(function, "dynamodb:DescribeTable")  # required by pynamodb

        import_status_endpoint_lambda = LambdaEndpoint(
            self,
            "import-status",
            package_name="import_status",
            deploy_env=deploy_env,
            botocore_lambda_layer=botocore_lambda_layer,
        )

        import_status_endpoint_lambda.grant_invoke(api_users_role)  # type: ignore[arg-type]

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

        grant_parameter_read_access(
            {
                datasets_table.name_parameter: [
                    datasets_endpoint_lambda,
                    dataset_versions_endpoint_lambda,
                ],
                validation_results_table.name_parameter: [import_status_endpoint_lambda],
                state_machine_parameter: [dataset_versions_endpoint_lambda],
            }
        )

        ############################################################################################
        # ### RAW S3 API ###########################################################################
        ############################################################################################

        storage_bucket.grant_read(s3_users_role)  # type: ignore[arg-type]

        Tags.of(self).add("ApplicationLayer", "api")  # type: ignore[arg-type]
