"""
Data Lake processing stack.
"""
from typing import Any

from aws_cdk import aws_dynamodb, aws_iam, aws_s3, aws_ssm, aws_stepfunctions
from aws_cdk.core import Construct, Stack

from backend.environment import ENV
from backend.parameter_store import ParameterName
from backend.validation_results_model import ValidationOutcomeIdx

from .common import grant_parameter_read_access
from .constructs.batch_job_queue import BatchJobQueue
from .constructs.batch_submit_job_task import BatchSubmitJobTask
from .constructs.import_file_function import ImportFileFunction
from .constructs.lambda_task import LambdaTask
from .constructs.table import Table


class ProcessingStack(Stack):
    """Data Lake processing stack definition."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        scope: Construct,
        stack_id: str,
        deploy_env: str,
        storage_bucket: aws_s3.Bucket,
        storage_bucket_parameter: aws_ssm.StringParameter,
        **kwargs: Any,
    ) -> None:
        # pylint: disable=too-many-locals
        super().__init__(scope, stack_id, **kwargs)

        self.deploy_env = deploy_env
        self.application_layer = "data-processing"

        ############################################################################################
        # PROCESSING ASSETS TABLE
        processing_assets_table = Table(
            self,
            f"{ENV}-processing-assets",
            deploy_env=self.deploy_env,
            application_layer=self.application_layer,
            parameter_name=ParameterName.PROCESSING_ASSETS_TABLE_NAME,
        )

        self.validation_results_table = Table(
            self,
            f"{ENV}-validation-results",
            deploy_env=self.deploy_env,
            application_layer=self.application_layer,
            parameter_name=ParameterName.VALIDATION_RESULTS_TABLE_NAME,
        )

        self.validation_results_table.add_global_secondary_index(
            index_name=ValidationOutcomeIdx.Meta.index_name,
            partition_key=aws_dynamodb.Attribute(
                name=ValidationOutcomeIdx.pk.attr_name, type=aws_dynamodb.AttributeType.STRING
            ),
            sort_key=aws_dynamodb.Attribute(
                name=ValidationOutcomeIdx.result.attr_name, type=aws_dynamodb.AttributeType.STRING
            ),
        )

        ############################################################################################
        # BATCH JOB DEPENDENCIES
        batch_job_queue = BatchJobQueue(
            self,
            "batch-job-queue",
            deploy_env=self.deploy_env,
            processing_assets_table=processing_assets_table,
        ).job_queue

        s3_read_only_access_policy = aws_iam.ManagedPolicy.from_aws_managed_policy_name(
            "AmazonS3ReadOnlyAccess"
        )

        ############################################################################################
        # STATE MACHINE TASKS
        check_stac_metadata_job_task = BatchSubmitJobTask(
            self,
            "check-stac-metadata-task",
            deploy_env=self.deploy_env,
            directory="check_stac_metadata",
            s3_policy=s3_read_only_access_policy,
            job_queue=batch_job_queue,
            payload_object={
                "dataset_id.$": "$.dataset_id",
                "version_id.$": "$.version_id",
                "type.$": "$.type",
                "metadata_url.$": "$.metadata_url",
            },
            container_overrides_command=[
                "--dataset-id",
                "Ref::dataset_id",
                "--version-id",
                "Ref::version_id",
                "--metadata-url",
                "Ref::metadata_url",
            ],
        )
        for table in [processing_assets_table, self.validation_results_table]:
            table.grant_read_write_data(
                check_stac_metadata_job_task.job_role  # type: ignore[arg-type]
            )
            table.grant(
                check_stac_metadata_job_task.job_role,  # type: ignore[arg-type]
                "dynamodb:DescribeTable",
            )

        content_iterator_task = LambdaTask(
            self,
            "content-iterator-task",
            directory="content_iterator",
            result_path="$.content",
            application_layer=self.application_layer,
            extra_environment={"DEPLOY_ENV": self.deploy_env},
        )

        check_files_checksums_directory = "check_files_checksums"
        check_files_checksums_default_payload_object = {
            "dataset_id.$": "$.dataset_id",
            "version_id.$": "$.version_id",
            "type.$": "$.type",
            "metadata_url.$": "$.metadata_url",
            "first_item.$": "$.content.first_item",
        }
        check_files_checksums_single_task = BatchSubmitJobTask(
            self,
            "check-files-checksums-single-task",
            deploy_env=self.deploy_env,
            directory=check_files_checksums_directory,
            s3_policy=s3_read_only_access_policy,
            job_queue=batch_job_queue,
            payload_object=check_files_checksums_default_payload_object,
            container_overrides_command=[
                "--dataset-id",
                "Ref::dataset_id",
                "--version-id",
                "Ref::version_id",
                "--first-item",
                "Ref::first_item",
            ],
        )
        array_size = int(aws_stepfunctions.JsonPath.number_at("$.content.iteration_size"))
        check_files_checksums_array_task = BatchSubmitJobTask(
            self,
            "check-files-checksums-array-task",
            deploy_env=self.deploy_env,
            directory=check_files_checksums_directory,
            s3_policy=s3_read_only_access_policy,
            job_queue=batch_job_queue,
            payload_object=check_files_checksums_default_payload_object,
            container_overrides_command=[
                "--dataset-id",
                "Ref::dataset_id",
                "--version-id",
                "Ref::version_id",
                "--first-item",
                "Ref::first_item",
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
            self.validation_results_table.grant_read_write_data(writer)  # type: ignore[arg-type]
            self.validation_results_table.grant(
                writer, "dynamodb:DescribeTable"  # type: ignore[arg-type]
            )

        validation_summary_task = LambdaTask(
            self,
            "validation-summary-task",
            directory="validation_summary",
            result_path="$.validation",
            application_layer=self.application_layer,
            extra_environment={"DEPLOY_ENV": self.deploy_env},
        )
        self.validation_results_table.grant_read_data(validation_summary_task.lambda_function)
        self.validation_results_table.grant(
            validation_summary_task.lambda_function, "dynamodb:DescribeTable"
        )

        validation_failure_lambda_invoke = LambdaTask(
            self,
            "validation-failure-task",
            directory="validation_failure",
            result_path=aws_stepfunctions.JsonPath.DISCARD,
            application_layer=self.application_layer,
        ).lambda_invoke

        self.import_dataset_role = aws_iam.Role(
            self,
            "import-dataset",
            assumed_by=aws_iam.ServicePrincipal(  # type: ignore[arg-type]
                "batchoperations.s3.amazonaws.com"
            ),
        )

        import_asset_file_function = ImportFileFunction(
            self,
            directory="import_asset_file",
            application_layer=self.application_layer,
            invoker=self.import_dataset_role,
            deploy_env=self.deploy_env,
        )
        import_metadata_file_function = ImportFileFunction(
            self,
            directory="import_metadata_file",
            application_layer=self.application_layer,
            invoker=self.import_dataset_role,
            deploy_env=self.deploy_env,
        )

        for storage_writer in [
            self.import_dataset_role,
            import_asset_file_function.role,
            import_metadata_file_function.role,
        ]:
            storage_bucket.grant_read_write(storage_writer)  # type: ignore[arg-type]

        import_dataset_task = LambdaTask(
            self,
            "import-dataset-task",
            directory="import_dataset",
            result_path="$.import_dataset",
            application_layer=self.application_layer,
            extra_environment={"DEPLOY_ENV": self.deploy_env},
        )

        assert import_dataset_task.lambda_function.role is not None
        import_dataset_task.lambda_function.role.add_to_policy(
            aws_iam.PolicyStatement(
                resources=[self.import_dataset_role.role_arn],
                actions=["iam:PassRole"],
            ),
        )
        import_dataset_task.lambda_function.role.add_to_policy(
            aws_iam.PolicyStatement(resources=["*"], actions=["s3:CreateJob"])
        )

        storage_bucket.grant_read_write(import_dataset_task.lambda_function)

        processing_assets_table.grant_read_data(import_dataset_task.lambda_function)
        processing_assets_table.grant(import_dataset_task.lambda_function, "dynamodb:DescribeTable")

        # Parameters
        import_asset_file_function_arn_parameter = aws_ssm.StringParameter(
            self,
            "import asset file function arn",
            string_value=import_asset_file_function.function_arn,
            description=f"Import asset file function ARN for {self.deploy_env}",
            parameter_name=ParameterName.IMPORT_ASSET_FILE_FUNCTION_TASK_ARN.value,
        )
        import_metadata_file_function_arn_parameter = aws_ssm.StringParameter(
            self,
            "import metadata file function arn",
            string_value=import_metadata_file_function.function_arn,
            description=f"Import metadata file function ARN for {self.deploy_env}",
            parameter_name=ParameterName.IMPORT_METADATA_FILE_FUNCTION_TASK_ARN.value,
        )

        import_dataset_role_arn_parameter = aws_ssm.StringParameter(
            self,
            "import dataset role arn",
            string_value=self.import_dataset_role.role_arn,
            description=f"Import dataset role ARN for {self.deploy_env}",
            parameter_name=ParameterName.IMPORT_DATASET_ROLE_ARN.value,
        )

        grant_parameter_read_access(
            {
                import_asset_file_function_arn_parameter: [import_dataset_task.lambda_function],
                import_dataset_role_arn_parameter: [import_dataset_task.lambda_function],
                import_metadata_file_function_arn_parameter: [import_dataset_task.lambda_function],
                processing_assets_table.name_parameter: [
                    check_files_checksums_array_task.job_role,  # type: ignore[list-item]
                    check_files_checksums_single_task.job_role,  # type: ignore[list-item]
                    check_stac_metadata_job_task.job_role,  # type: ignore[list-item]
                    content_iterator_task.lambda_function,
                    import_dataset_task.lambda_function,
                ],
                storage_bucket_parameter: [
                    import_dataset_task.lambda_function,
                    import_asset_file_function,
                    import_metadata_file_function,
                ],
                self.validation_results_table.name_parameter: [
                    check_files_checksums_array_task.job_role,  # type: ignore[list-item]
                    check_files_checksums_single_task.job_role,  # type: ignore[list-item]
                    check_stac_metadata_job_task.job_role,  # type: ignore[list-item]
                    validation_summary_task.lambda_function,
                ],
            }
        )

        success_task = aws_stepfunctions.Succeed(self, "success")

        ############################################################################################
        # STATE MACHINE
        dataset_version_creation_definition = (
            check_stac_metadata_job_task.batch_submit_job.next(content_iterator_task.lambda_invoke)
            .next(
                aws_stepfunctions.Choice(  # type: ignore[arg-type]
                    self, "check_files_checksums_maybe_array"
                )
                .when(
                    aws_stepfunctions.Condition.number_equals("$.content.iteration_size", 1),
                    check_files_checksums_single_task.batch_submit_job,
                )
                .otherwise(check_files_checksums_array_task.batch_submit_job)
                .afterwards()
            )
            .next(
                aws_stepfunctions.Choice(self, "content_iteration_finished")
                .when(
                    aws_stepfunctions.Condition.number_equals("$.content.next_item", -1),
                    validation_summary_task.lambda_invoke.next(
                        aws_stepfunctions.Choice(  # type: ignore[arg-type]
                            self, "validation_successful"
                        )
                        .when(
                            aws_stepfunctions.Condition.boolean_equals(
                                "$.validation.success", True
                            ),
                            import_dataset_task.lambda_invoke.next(
                                success_task  # type: ignore[arg-type]
                            ),
                        )
                        .otherwise(validation_failure_lambda_invoke)
                    ),
                )
                .otherwise(content_iterator_task.lambda_invoke)
            )
        )

        self.state_machine = aws_stepfunctions.StateMachine(
            self,
            f"{self.deploy_env}-dataset-version-creation",
            definition=dataset_version_creation_definition,  # type: ignore[arg-type]
        )

        self.state_machine_parameter = aws_ssm.StringParameter(
            self,
            "state machine arn",
            description=f"State machine ARN for {self.deploy_env}",
            parameter_name=ParameterName.DATASET_VERSION_CREATION_STEP_FUNCTION_ARN.value,
            string_value=self.state_machine.state_machine_arn,
        )
