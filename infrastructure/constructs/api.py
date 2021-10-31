from aws_cdk import aws_iam, aws_lambda_python, aws_s3, aws_sqs, aws_ssm, aws_stepfunctions
from aws_cdk.core import Construct, Tags

from geostore.resources import ResourceName

from .common import grant_parameter_read_access
from .lambda_endpoint import LambdaEndpoint
from .roles import MAX_SESSION_DURATION
from .s3_policy import ALLOW_DESCRIBE_ANY_S3_JOB
from .table import Table


class API(Construct):
    def __init__(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        scope: Construct,
        stack_id: str,
        *,
        botocore_lambda_layer: aws_lambda_python.PythonLayerVersion,
        datasets_table: Table,
        env_name: str,
        principal: aws_iam.PrincipalBase,
        state_machine: aws_stepfunctions.StateMachine,
        state_machine_parameter: aws_ssm.StringParameter,
        sqs_queue: aws_sqs.Queue,
        sqs_queue_parameter: aws_ssm.StringParameter,
        storage_bucket: aws_s3.Bucket,
        validation_results_table: Table,
    ) -> None:
        super().__init__(scope, stack_id)

        ############################################################################################
        # ### API ENDPOINTS ########################################################################
        ############################################################################################

        api_users_role = aws_iam.Role(
            self,
            "api-users-role",
            role_name=ResourceName.API_USERS_ROLE_NAME.value,
            assumed_by=principal,  # type: ignore[arg-type]
            max_session_duration=MAX_SESSION_DURATION,
        )

        datasets_endpoint_lambda = LambdaEndpoint(
            self,
            "datasets",
            package_name="datasets",
            env_name=env_name,
            users_role=api_users_role,
            botocore_lambda_layer=botocore_lambda_layer,
        )

        dataset_versions_endpoint_lambda = LambdaEndpoint(
            self,
            "dataset-versions",
            package_name="dataset_versions",
            env_name=env_name,
            users_role=api_users_role,
            botocore_lambda_layer=botocore_lambda_layer,
        )

        state_machine.grant_start_execution(dataset_versions_endpoint_lambda)

        storage_bucket.grant_read_write(datasets_endpoint_lambda)

        sqs_queue.grant_send_messages(datasets_endpoint_lambda)

        for function in [datasets_endpoint_lambda, dataset_versions_endpoint_lambda]:
            datasets_table.grant_read_write_data(function)
            datasets_table.grant(function, "dynamodb:DescribeTable")  # required by pynamodb

        import_status_endpoint_lambda = LambdaEndpoint(
            self,
            "import-status",
            package_name="import_status",
            env_name=env_name,
            users_role=api_users_role,
            botocore_lambda_layer=botocore_lambda_layer,
        )

        validation_results_table.grant_read_data(import_status_endpoint_lambda)
        validation_results_table.grant(
            import_status_endpoint_lambda, "dynamodb:DescribeTable"
        )  # required by pynamodb

        state_machine.grant_read(import_status_endpoint_lambda)
        import_status_endpoint_lambda.add_to_role_policy(ALLOW_DESCRIBE_ANY_S3_JOB)

        grant_parameter_read_access(
            {
                datasets_table.name_parameter: [
                    datasets_endpoint_lambda,
                    dataset_versions_endpoint_lambda,
                ],
                validation_results_table.name_parameter: [import_status_endpoint_lambda],
                state_machine_parameter: [dataset_versions_endpoint_lambda],
                sqs_queue_parameter: [datasets_endpoint_lambda],
            }
        )

        ############################################################################################
        # ### S3 API ###############################################################################
        ############################################################################################

        s3_users_role = aws_iam.Role(
            self,
            "s3-users-role",
            role_name=ResourceName.S3_USERS_ROLE_NAME.value,
            assumed_by=principal,  # type: ignore[arg-type]
            max_session_duration=MAX_SESSION_DURATION,
        )
        storage_bucket.grant_read(s3_users_role)  # type: ignore[arg-type]

        Tags.of(self).add("ApplicationLayer", "api")  # type: ignore[arg-type]
