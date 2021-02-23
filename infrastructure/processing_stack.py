"""
Data Lake processing stack.
"""
from typing import Any

from aws_cdk import aws_iam, aws_s3, aws_ssm, aws_stepfunctions, core
from aws_cdk.aws_iam import PolicyStatement

from backend.dataset_versions.create import DATASET_VERSION_CREATION_STEP_FUNCTION
from backend.utils import ResourceName
from backend.import_dataset.task import (
    S3_BATCH_COPY_ROLE_PARAMETER,
    STORAGE_BUCKET_PARAMETER,
)
from .constructs.batch_job_queue import BatchJobQueue
from .constructs.batch_submit_job_task import BatchSubmitJobTask
from .constructs.lambda_task import LambdaTask
from .constructs.table import Table
from .staging_stack import STAGING_BUCKET_PARAMETER


class ProcessingStack(core.Stack):
    """Data Lake processing stack definition."""

    def __init__(
        self,
        scope: core.Construct,
        stack_id: str,
        deploy_env: str,
        **kwargs: Any,
    ) -> None:
        # pylint: disable=too-many-locals
        super().__init__(scope, stack_id, **kwargs)

        application_layer = "data-processing"

        ############################################################################################
        # PROCESSING ASSETS TABLE
        processing_assets_table = Table(
            self,
            ResourceName.PROCESSING_ASSETS_TABLE_NAME.value,
            deploy_env=deploy_env,
            application_layer=application_layer,
        )

        ############################################################################################
        # BATCH JOB DEPENDENCIES
        batch_job_queue = BatchJobQueue(
            self,
            "batch_job_queue",
            deploy_env=deploy_env,
            processing_assets_table=processing_assets_table,
        ).job_queue

        s3_read_only_access_policy = aws_iam.ManagedPolicy.from_aws_managed_policy_name(
            "AmazonS3ReadOnlyAccess"
        )

        ############################################################################################
        # STATE MACHINE TASKS
        check_stac_metadata_job_task = BatchSubmitJobTask(
            self,
            "check_stac_metadata_task",
            deploy_env=deploy_env,
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
        processing_assets_table.grant_read_write_data(
            check_stac_metadata_job_task.job_role  # type: ignore[arg-type]
        )
        processing_assets_table.grant(
            check_stac_metadata_job_task.job_role,  # type: ignore[arg-type]
            "dynamodb:DescribeTable",
        )

        content_iterator_task = LambdaTask(
            self,
            "content_iterator_task",
            directory="content_iterator",
            result_path="$.content",
            application_layer=application_layer,
            extra_environment={"DEPLOY_ENV": deploy_env},
        )

        check_files_checksums_directory = "check_files_checksums"
        check_files_checksums_default_payload_object = {
            "dataset_id.$": "$.dataset_id",
            "version_id.$": "$.version_id",
            "type.$": "$.type",
            "metadata_url.$": "$.metadata_url",
            "first_item.$": "$.content.first_item",
        }
        check_files_checksums_default_container_overrides_command = [
            "--dataset-id",
            "Ref::dataset_id",
            "--version-id",
            "Ref::version_id",
            "--first-item",
            "Ref::first_item",
        ]
        check_files_checksums_single_task = BatchSubmitJobTask(
            self,
            "check_files_checksums_single_task",
            deploy_env=deploy_env,
            directory=check_files_checksums_directory,
            s3_policy=s3_read_only_access_policy,
            job_queue=batch_job_queue,
            payload_object=check_files_checksums_default_payload_object,
            container_overrides_command=check_files_checksums_default_container_overrides_command,
        )
        array_size = int(aws_stepfunctions.JsonPath.number_at("$.content.iteration_size"))
        check_files_checksums_array_task = BatchSubmitJobTask(
            self,
            "check_files_checksums_array_task",
            deploy_env=deploy_env,
            directory=check_files_checksums_directory,
            s3_policy=s3_read_only_access_policy,
            job_queue=batch_job_queue,
            payload_object=check_files_checksums_default_payload_object,
            container_overrides_command=check_files_checksums_default_container_overrides_command,
            array_size=array_size,
        )

        processing_assets_table_readers = [
            content_iterator_task.lambda_function,
            check_files_checksums_single_task.job_role,
            check_files_checksums_array_task.job_role,
        ]
        for reader in processing_assets_table_readers:
            processing_assets_table.grant_read_data(reader)  # type: ignore[arg-type]
            processing_assets_table.grant(
                reader, "dynamodb:DescribeTable"  # type: ignore[arg-type]
            )

        validation_summary_lambda_invoke = LambdaTask(
            self,
            "validation_summary_task",
            directory="validation_summary",
            result_path="$.validation",
            application_layer=application_layer,
        ).lambda_invoke

        validation_failure_lambda_invoke = LambdaTask(
            self,
            "validation_failure_task",
            directory="validation_failure",
            result_path=aws_stepfunctions.JsonPath.DISCARD,
            application_layer=application_layer,
        ).lambda_invoke

        storage_bucket_arn = aws_ssm.StringParameter.from_string_parameter_attributes(
            self,
            "storage-bucket-arn",
            parameter_name=STORAGE_BUCKET_PARAMETER,
        )

        staging_bucket_arn = aws_ssm.StringParameter.from_string_parameter_attributes(
            self,
            "staging-bucket-arn",
            parameter_name=STAGING_BUCKET_PARAMETER,
        )

        storage_bucket = aws_s3.Bucket.from_bucket_arn(
            self, "storage-bucket", storage_bucket_arn.string_value
        )

        s3_batch_copy_role = aws_iam.Role(
            self,
            "s3_batch_copy_role",
            assumed_by=aws_iam.ServicePrincipal("batchoperations.s3.amazonaws.com"),
        )
        s3_batch_copy_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:GetObjectAcl",
                    "s3:GetObjectTagging",
                ],
                resources=[
                    staging_bucket_arn.string_value + "/*",
                ],
            ),
        )
        s3_batch_copy_role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=[
                    "s3:PutObject",
                    "s3:PutObjectAcl",
                    "s3:PutObjectTagging",
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:GetBucketLocation",
                ],
                resources=[
                    storage_bucket_arn.string_value + "/*",
                ],
            )
        )

        aws_ssm.StringParameter(
            self,
            "s3-batch-copy-role-arn",
            description=f"S3 Batch Copy Role ARN for {deploy_env}",
            parameter_name=S3_BATCH_COPY_ROLE_PARAMETER,
            string_value=s3_batch_copy_role.role_arn,
        )

        import_dataset_task = LambdaTask(
            self,
            "import_dataset_task",
            directory="import_dataset",
            result_path=aws_stepfunctions.JsonPath.DISCARD,
            permission_functions=[
                storage_bucket_arn.grant_read,
                storage_bucket.grant_read_write,
                processing_assets_table.grant_read_data,
            ],
            application_layer=application_layer,
            extra_environment={"DEPLOY_ENV": deploy_env},
        )

        assert import_dataset_task.lambda_function.role is not None
        import_dataset_task.lambda_function.role.add_to_policy(
            PolicyStatement(
                resources=["*"],
                actions=["s3:CreateJob", "iam:PassRole"],
            ),
        )

        processing_assets_table.grant(
            import_dataset_task.lambda_function.role, "dynamodb:DescribeTable"
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
                    validation_summary_lambda_invoke.next(
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

        state_machine = aws_stepfunctions.StateMachine(
            self,
            f"{deploy_env}-dataset-version-creation",
            definition=dataset_version_creation_definition,  # type: ignore[arg-type]
        )

        aws_ssm.StringParameter(
            self,
            "step-func-statemachine-arn",
            description=f"Step Function State Machine ARN for {deploy_env}",
            parameter_name=DATASET_VERSION_CREATION_STEP_FUNCTION,
            string_value=state_machine.state_machine_arn,
        )
