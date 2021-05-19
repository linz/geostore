from aws_cdk import aws_dynamodb, aws_iam, aws_lambda_python, aws_s3, aws_ssm, aws_stepfunctions
from aws_cdk.aws_stepfunctions import Wait, WaitTime
from aws_cdk.core import Construct, Duration, Tags

from backend.api_keys import SUCCESS_KEY
from backend.content_iterator.task import (
    ASSETS_TABLE_NAME_KEY,
    CONTENT_KEY,
    FIRST_ITEM_KEY,
    ITERATION_SIZE_KEY,
    NEXT_ITEM_KEY,
    RESULTS_TABLE_NAME_KEY,
)
from backend.import_status.get import IMPORT_DATASET_KEY
from backend.parameter_store import ParameterName
from backend.step_function import (
    ASSET_UPLOAD_KEY,
    DATASET_ID_KEY,
    METADATA_UPLOAD_KEY,
    METADATA_URL_KEY,
    VALIDATION_KEY,
    VERSION_ID_KEY,
)

from .batch_job_queue import BatchJobQueue
from .batch_submit_job_task import BatchSubmitJobTask
from .common import grant_parameter_read_access
from .import_file_function import ImportFileFunction
from .lambda_task import LambdaTask
from .s3_policy import ALLOW_DESCRIBE_ANY_S3_JOB
from .table import Table


class Processing(Construct):
    def __init__(
        self,
        scope: Construct,
        stack_id: str,
        *,
        botocore_lambda_layer: aws_lambda_python.PythonLayerVersion,
        datasets_table: Table,
        deploy_env: str,
        storage_bucket: aws_s3.Bucket,
        validation_results_table: Table,
    ) -> None:
        # pylint: disable=too-many-locals
        super().__init__(scope, stack_id)

        ############################################################################################
        # PROCESSING ASSETS TABLE
        processing_assets_table = Table(
            self,
            f"{deploy_env}-processing-assets",
            deploy_env=deploy_env,
            parameter_name=ParameterName.PROCESSING_ASSETS_TABLE_NAME,
            sort_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
        )

        ############################################################################################
        # BATCH JOB DEPENDENCIES
        batch_job_queue = BatchJobQueue(
            self,
            "batch-job-queue",
            deploy_env=deploy_env,
            processing_assets_table=processing_assets_table,
        ).job_queue

        s3_read_only_access_policy = aws_iam.ManagedPolicy.from_aws_managed_policy_name(
            "AmazonS3ReadOnlyAccess"
        )

        ############################################################################################
        # STATE MACHINE TASKS

        check_stac_metadata_task = LambdaTask(
            self,
            "check-stac-metadata-task",
            directory="check_stac_metadata",
            botocore_lambda_layer=botocore_lambda_layer,
            extra_environment={"DEPLOY_ENV": deploy_env},
        )
        assert check_stac_metadata_task.lambda_function.role
        check_stac_metadata_task.lambda_function.role.add_managed_policy(
            policy=s3_read_only_access_policy
        )

        for table in [processing_assets_table, validation_results_table]:
            table.grant_read_write_data(check_stac_metadata_task.lambda_function)
            table.grant(
                check_stac_metadata_task.lambda_function,
                "dynamodb:DescribeTable",
            )

        content_iterator_task = LambdaTask(
            self,
            "content-iterator-task",
            directory="content_iterator",
            botocore_lambda_layer=botocore_lambda_layer,
            result_path=f"$.{CONTENT_KEY}",
            extra_environment={"DEPLOY_ENV": deploy_env},
        )

        check_files_checksums_directory = "check_files_checksums"
        check_files_checksums_default_payload_object = {
            f"{DATASET_ID_KEY}.$": f"$.{DATASET_ID_KEY}",
            f"{VERSION_ID_KEY}.$": f"$.{VERSION_ID_KEY}",
            f"{METADATA_URL_KEY}.$": f"$.{METADATA_URL_KEY}",
            f"{FIRST_ITEM_KEY}.$": f"$.{CONTENT_KEY}.{FIRST_ITEM_KEY}",
            f"{ASSETS_TABLE_NAME_KEY}.$": f"$.{CONTENT_KEY}.{ASSETS_TABLE_NAME_KEY}",
            f"{RESULTS_TABLE_NAME_KEY}.$": f"$.{CONTENT_KEY}.{RESULTS_TABLE_NAME_KEY}",
        }
        check_files_checksums_single_task = BatchSubmitJobTask(
            self,
            "check-files-checksums-single-task",
            deploy_env=deploy_env,
            directory=check_files_checksums_directory,
            s3_policy=s3_read_only_access_policy,
            job_queue=batch_job_queue,
            payload_object=check_files_checksums_default_payload_object,
            container_overrides_command=[
                "--dataset-id",
                f"Ref::{DATASET_ID_KEY}",
                "--version-id",
                f"Ref::{VERSION_ID_KEY}",
                "--first-item",
                f"Ref::{FIRST_ITEM_KEY}",
                "--assets-table-name",
                f"Ref::{ASSETS_TABLE_NAME_KEY}",
                "--results-table-name",
                f"Ref::{RESULTS_TABLE_NAME_KEY}",
            ],
        )
        array_size = int(
            aws_stepfunctions.JsonPath.number_at(f"$.{CONTENT_KEY}.{ITERATION_SIZE_KEY}")
        )
        check_files_checksums_array_task = BatchSubmitJobTask(
            self,
            "check-files-checksums-array-task",
            deploy_env=deploy_env,
            directory=check_files_checksums_directory,
            s3_policy=s3_read_only_access_policy,
            job_queue=batch_job_queue,
            payload_object=check_files_checksums_default_payload_object,
            container_overrides_command=[
                "--dataset-id",
                f"Ref::{DATASET_ID_KEY}",
                "--version-id",
                f"Ref::{VERSION_ID_KEY}",
                "--first-item",
                f"Ref::{FIRST_ITEM_KEY}",
                "--assets-table-name",
                f"Ref::{ASSETS_TABLE_NAME_KEY}",
                "--results-table-name",
                f"Ref::{RESULTS_TABLE_NAME_KEY}",
            ],
            array_size=array_size,
        )

        for reader in [
            content_iterator_task.lambda_function,
            check_files_checksums_single_task.job_role,
            check_files_checksums_array_task.job_role,
        ]:
            processing_assets_table.grant_read_data(reader)  # type: ignore[arg-type]
            processing_assets_table.grant(
                reader, "dynamodb:DescribeTable"  # type: ignore[arg-type]
            )

        for writer in [
            check_files_checksums_single_task.job_role,
            check_files_checksums_array_task.job_role,
        ]:
            validation_results_table.grant_read_write_data(writer)  # type: ignore[arg-type]
            validation_results_table.grant(
                writer, "dynamodb:DescribeTable"  # type: ignore[arg-type]
            )

        validation_summary_task = LambdaTask(
            self,
            "validation-summary-task",
            directory="validation_summary",
            botocore_lambda_layer=botocore_lambda_layer,
            result_path=f"$.{VALIDATION_KEY}",
            extra_environment={"DEPLOY_ENV": deploy_env},
        )
        validation_results_table.grant_read_data(validation_summary_task.lambda_function)
        validation_results_table.grant(
            validation_summary_task.lambda_function, "dynamodb:DescribeTable"
        )

        validation_failure_lambda_invoke = LambdaTask(
            self,
            "validation-failure-task",
            directory="validation_failure",
            botocore_lambda_layer=botocore_lambda_layer,
            result_path=aws_stepfunctions.JsonPath.DISCARD,
        )

        import_dataset_role = aws_iam.Role(
            self,
            "import-dataset",
            assumed_by=aws_iam.ServicePrincipal(  # type: ignore[arg-type]
                "batchoperations.s3.amazonaws.com"
            ),
        )

        import_asset_file_function = ImportFileFunction(
            self,
            directory="import_asset_file",
            invoker=import_dataset_role,
            deploy_env=deploy_env,
            botocore_lambda_layer=botocore_lambda_layer,
        )
        import_metadata_file_function = ImportFileFunction(
            self,
            directory="import_metadata_file",
            invoker=import_dataset_role,
            deploy_env=deploy_env,
            botocore_lambda_layer=botocore_lambda_layer,
        )

        for storage_writer in [
            import_dataset_role,
            import_asset_file_function,
            import_metadata_file_function,
        ]:
            storage_bucket.grant_read_write(storage_writer)  # type: ignore[arg-type]

        import_dataset_task = LambdaTask(
            self,
            "import-dataset-task",
            directory="import_dataset",
            botocore_lambda_layer=botocore_lambda_layer,
            result_path=f"$.{IMPORT_DATASET_KEY}",
            extra_environment={"DEPLOY_ENV": deploy_env},
        )

        import_dataset_task.lambda_function.add_to_role_policy(
            aws_iam.PolicyStatement(
                resources=[import_dataset_role.role_arn],
                actions=["iam:PassRole"],
            ),
        )
        import_dataset_task.lambda_function.add_to_role_policy(
            aws_iam.PolicyStatement(resources=["*"], actions=["s3:CreateJob"])
        )

        storage_bucket.grant_read_write(import_dataset_task.lambda_function)

        for table in [datasets_table, processing_assets_table]:
            table.grant_read_data(import_dataset_task.lambda_function)
            table.grant(import_dataset_task.lambda_function, "dynamodb:DescribeTable")

        # Import status check
        wait_before_upload_status_check = Wait(
            self,
            "wait-before-upload-status-check",
            time=WaitTime.duration(Duration.seconds(10)),
        )
        upload_status_task = LambdaTask(
            self,
            "upload-status",
            directory="upload_status",
            botocore_lambda_layer=botocore_lambda_layer,
            result_path="$.upload_status",
            extra_environment={"DEPLOY_ENV": deploy_env},
        )
        validation_results_table.grant_read_data(upload_status_task.lambda_function)
        validation_results_table.grant(upload_status_task.lambda_function, "dynamodb:DescribeTable")

        upload_status_task.lambda_function.add_to_role_policy(ALLOW_DESCRIBE_ANY_S3_JOB)

        # Parameters
        import_asset_file_function_arn_parameter = aws_ssm.StringParameter(
            self,
            "import asset file function arn",
            string_value=import_asset_file_function.function_arn,
            description=f"Import asset file function ARN for {deploy_env}",
            parameter_name=ParameterName.PROCESSING_IMPORT_ASSET_FILE_FUNCTION_TASK_ARN.value,
        )
        import_metadata_file_function_arn_parameter = aws_ssm.StringParameter(
            self,
            "import metadata file function arn",
            string_value=import_metadata_file_function.function_arn,
            description=f"Import metadata file function ARN for {deploy_env}",
            parameter_name=ParameterName.PROCESSING_IMPORT_METADATA_FILE_FUNCTION_TASK_ARN.value,
        )

        import_dataset_role_arn_parameter = aws_ssm.StringParameter(
            self,
            "import dataset role arn",
            string_value=import_dataset_role.role_arn,
            description=f"Import dataset role ARN for {deploy_env}",
            parameter_name=ParameterName.PROCESSING_IMPORT_DATASET_ROLE_ARN.value,
        )

        grant_parameter_read_access(
            {
                datasets_table.name_parameter: [import_dataset_task.lambda_function],
                import_asset_file_function_arn_parameter: [import_dataset_task.lambda_function],
                import_dataset_role_arn_parameter: [import_dataset_task.lambda_function],
                import_metadata_file_function_arn_parameter: [import_dataset_task.lambda_function],
                processing_assets_table.name_parameter: [
                    check_stac_metadata_task.lambda_function,
                    content_iterator_task.lambda_function,
                    import_dataset_task.lambda_function,
                ],
                validation_results_table.name_parameter: [
                    check_stac_metadata_task.lambda_function,
                    content_iterator_task.lambda_function,
                    validation_summary_task.lambda_function,
                    upload_status_task.lambda_function,
                ],
            }
        )

        success_task = aws_stepfunctions.Succeed(self, "success")
        fail_task = aws_stepfunctions.Fail(self, "fail")

        ############################################################################################
        # STATE MACHINE
        dataset_version_creation_definition = (
            check_stac_metadata_task.next(content_iterator_task)
            .next(
                aws_stepfunctions.Choice(  # type: ignore[arg-type]
                    self, "check_files_checksums_maybe_array"
                )
                .when(
                    aws_stepfunctions.Condition.number_equals(
                        f"$.{CONTENT_KEY}.{ITERATION_SIZE_KEY}", 1
                    ),
                    check_files_checksums_single_task.batch_submit_job,
                )
                .otherwise(check_files_checksums_array_task.batch_submit_job)
                .afterwards()
            )
            .next(
                aws_stepfunctions.Choice(self, "content_iteration_finished")
                .when(
                    aws_stepfunctions.Condition.number_equals(
                        f"$.{CONTENT_KEY}.{NEXT_ITEM_KEY}", -1
                    ),
                    validation_summary_task.next(
                        aws_stepfunctions.Choice(  # type: ignore[arg-type]
                            self, "validation_successful"
                        )
                        .when(
                            aws_stepfunctions.Condition.boolean_equals(
                                f"$.{VALIDATION_KEY}.{SUCCESS_KEY}", True
                            ),
                            import_dataset_task.next(
                                wait_before_upload_status_check  # type: ignore[arg-type]
                            )
                            .next(upload_status_task)
                            .next(
                                aws_stepfunctions.Choice(
                                    self, "import_completed"  # type: ignore[arg-type]
                                )
                                .when(
                                    aws_stepfunctions.Condition.and_(
                                        aws_stepfunctions.Condition.string_equals(
                                            f"$.upload_status.{ASSET_UPLOAD_KEY}.status", "Complete"
                                        ),
                                        aws_stepfunctions.Condition.string_equals(
                                            f"$.upload_status.{METADATA_UPLOAD_KEY}.status",
                                            "Complete",
                                        ),
                                    ),
                                    success_task,  # type: ignore[arg-type]
                                )
                                .when(
                                    aws_stepfunctions.Condition.or_(
                                        aws_stepfunctions.Condition.string_equals(
                                            f"$.upload_status.{ASSET_UPLOAD_KEY}.status",
                                            "Cancelled",
                                        ),
                                        aws_stepfunctions.Condition.string_equals(
                                            f"$.upload_status.{ASSET_UPLOAD_KEY}.status", "Failed"
                                        ),
                                        aws_stepfunctions.Condition.string_equals(
                                            f"$.upload_status.{METADATA_UPLOAD_KEY}.status",
                                            "Cancelled",
                                        ),
                                        aws_stepfunctions.Condition.string_equals(
                                            f"$.upload_status.{METADATA_UPLOAD_KEY}.status",
                                            "Failed",
                                        ),
                                    ),
                                    fail_task,  # type: ignore[arg-type]
                                )
                                .otherwise(
                                    wait_before_upload_status_check  # type: ignore[arg-type]
                                )
                            ),
                        )
                        .otherwise(validation_failure_lambda_invoke)
                    ),
                )
                .otherwise(content_iterator_task)
            )
        )

        self.state_machine = aws_stepfunctions.StateMachine(
            self,
            f"{deploy_env}-dataset-version-creation",
            definition=dataset_version_creation_definition,  # type: ignore[arg-type]
        )

        self.state_machine_parameter = aws_ssm.StringParameter(
            self,
            "state machine arn",
            description=f"State machine ARN for {deploy_env}",
            parameter_name=ParameterName.PROCESSING_DATASET_VERSION_CREATION_STEP_FUNCTION_ARN.value,  # pylint:disable=line-too-long
            string_value=self.state_machine.state_machine_arn,
        )

        Tags.of(self).add("ApplicationLayer", "processing")  # type: ignore[arg-type]
