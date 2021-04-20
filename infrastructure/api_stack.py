"""
Data Lake AWS resources definitions.
"""
from os import environ
from typing import Any

from aws_cdk import aws_iam, aws_lambda_python, aws_s3, aws_ssm, aws_stepfunctions
from aws_cdk.core import Construct, Duration, Stack, Tags

from backend.resources import ResourceName

from .common import grant_parameter_read_access
from .constructs.lambda_endpoint import LambdaEndpoint
from .constructs.table import Table


class APIStack(Stack):
    """Data Lake stack definition."""

    def __init__(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        scope: Construct,
        stack_id: str,
        datasets_table: Table,
        validation_results_table: Table,
        deploy_env: str,
        state_machine: aws_stepfunctions.StateMachine,
        state_machine_parameter: aws_ssm.StringParameter,
        storage_bucket: aws_s3.Bucket,
        storage_bucket_parameter: aws_ssm.StringParameter,
        botocore_lambda_layer: aws_lambda_python.PythonLayerVersion,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, stack_id, **kwargs)

        if saml_provider_arn := environ.get("DATALAKE_SAML_IDENTITY_PROVIDER_ARN"):
            principal = aws_iam.FederatedPrincipal(
                federated=saml_provider_arn,
                assume_role_action="sts:AssumeRoleWithSAML",
                conditions={"StringEquals": {"SAML:aud": "https://signin.aws.amazon.com/saml"}},
            )

        else:
            principal = aws_iam.AccountPrincipal(  # type: ignore[assignment]
                account_id=aws_iam.AccountRootPrincipal().account_id
            )

        users_role = aws_iam.Role(
            self,
            "users-role",
            role_name=ResourceName.USERS_ROLE_NAME.value,
            assumed_by=principal,  # type: ignore[arg-type]
            max_session_duration=Duration.hours(12),
        )
        Tags.of(users_role).add("ApplicationLayer", "users")  # type: ignore[arg-type]

        ############################################################################################
        # ### API ENDPOINTS ########################################################################
        ############################################################################################

        datasets_endpoint_lambda = LambdaEndpoint(
            self,
            "datasets",
            package_name="datasets",
            deploy_env=deploy_env,
            users_role=users_role,
            botocore_lambda_layer=botocore_lambda_layer,
        ).lambda_function

        dataset_versions_endpoint_lambda = LambdaEndpoint(
            self,
            "dataset-versions",
            package_name="dataset_versions",
            deploy_env=deploy_env,
            users_role=users_role,
            botocore_lambda_layer=botocore_lambda_layer,
        ).lambda_function

        state_machine.grant_start_execution(dataset_versions_endpoint_lambda)

        storage_bucket.grant_read(datasets_endpoint_lambda)
        storage_bucket_parameter.grant_read(datasets_endpoint_lambda)

        for function in [datasets_endpoint_lambda, dataset_versions_endpoint_lambda]:
            datasets_table.grant_read_write_data(function)
            datasets_table.grant(function, "dynamodb:DescribeTable")  # required by pynamodb

        import_status_endpoint_lambda = LambdaEndpoint(
            self,
            "import-status",
            package_name="import_status",
            deploy_env=deploy_env,
            users_role=users_role,
            botocore_lambda_layer=botocore_lambda_layer,
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
