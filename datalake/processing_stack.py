"""
Data Lake processing stack.
"""
from typing import Any

from aws_cdk import aws_ec2, aws_iam, aws_stepfunctions, core

from .backend.processing.check_stac_metadata.task import PROCESSING_ASSETS_TABLE_NAME
from .constructs.batch_job_queue import BatchJobQueue
from .constructs.batch_submit_job_task import BatchSubmitJobTask
from .constructs.lambda_task import LambdaTask
from .constructs.table import Table


class ProcessingStack(core.Stack):
    """Data Lake processing stack definition."""

    def __init__(
        self,
        scope: core.Construct,
        stack_id: str,
        deploy_env: str,
        vpc: aws_ec2.IVpc,
        **kwargs: Any,
    ) -> None:
        # pylint: disable=too-many-locals
        super().__init__(scope, stack_id, **kwargs)

        application_layer = "data-processing"

        ############################################################################################
        # PROCESSING ASSETS TABLE
        processing_assets_table = Table(
            self,
            PROCESSING_ASSETS_TABLE_NAME,
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
            vpc=vpc,
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
        processing_assets_table.grant_read_write_data(check_stac_metadata_job_task.job_role)
        processing_assets_table.grant(
            check_stac_metadata_job_task.job_role, "dynamodb:DescribeTable"
        )
        check_stac_metadata_task = check_stac_metadata_job_task.batch_submit_job

        content_iterator_task = LambdaTask(
            self,
            "content_iterator_task",
            directory="content_iterator",
            result_path="$.content",
            permission_functions=[processing_assets_table.grant_read_data],
            application_layer=application_layer,
        ).lambda_invoke

        check_files_checksums_task = BatchSubmitJobTask(
            self,
            "check_files_checksums_task",
            deploy_env=deploy_env,
            directory="check_files_checksums",
            s3_policy=s3_read_only_access_policy,
            job_queue=batch_job_queue,
            payload_object={
                "dataset_id.$": "$.dataset_id",
                "version_id.$": "$.version_id",
                "type.$": "$.type",
                "metadata_url.$": "$.metadata_url",
                "first_item.$": "$.content.first_item",
            },
            container_overrides_environment={"BATCH_JOB_FIRST_ITEM_INDEX": "Ref::first_item"},
            array_size=aws_stepfunctions.JsonPath.number_at("$.content.iteration_size"),
            container_overrides_command=["--metadata-url", "Ref::metadata_url"],
        ).batch_submit_job

        validation_summary_task = LambdaTask(
            self,
            "validation_summary_task",
            directory="validation_summary",
            result_path="$.validation",
            application_layer=application_layer,
        ).lambda_invoke

        validation_failure_task = LambdaTask(
            self,
            "validation_failure_task",
            directory="validation_failure",
            result_path=aws_stepfunctions.JsonPath.DISCARD,
            application_layer=application_layer,
        ).lambda_invoke

        success_task = aws_stepfunctions.Succeed(self, "success")

        ############################################################################################
        # STATE MACHINE
        dataset_version_creation_definition = (
            check_stac_metadata_task.next(content_iterator_task)
            .next(check_files_checksums_task)
            .next(
                aws_stepfunctions.Choice(self, "content_iteration_finished")
                .when(
                    aws_stepfunctions.Condition.not_(
                        aws_stepfunctions.Condition.number_equals("$.content.next_item", -1)
                    ),
                    content_iterator_task,
                )
                .otherwise(
                    validation_summary_task.next(
                        aws_stepfunctions.Choice(self, "validation_successful")
                        .when(
                            aws_stepfunctions.Condition.boolean_equals(
                                "$.validation.success", True
                            ),
                            success_task,
                        )
                        .otherwise(validation_failure_task)
                    ),
                )
            )
        )

        aws_stepfunctions.StateMachine(
            self,
            "dataset-version-creation",
            definition=dataset_version_creation_definition,
        )
