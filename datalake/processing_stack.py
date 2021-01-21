"""
Data Lake processing stack.
"""
import textwrap
from typing import Any, List

from aws_cdk import (
    aws_batch,
    aws_dynamodb,
    aws_ec2,
    aws_ecs,
    aws_iam,
    aws_lambda,
    aws_stepfunctions,
    aws_stepfunctions_tasks,
    core,
)

JOB_DEFINITION_SUFFIX = "_job"


class ProcessingStack(core.Stack):
    """Data Lake processing stack definition."""

    # pylint: disable=too-many-locals
    def __init__(
        self,
        scope: core.Construct,
        stack_id: str,
        deploy_env: str,
        vpc: aws_ec2.IVpc,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, stack_id, **kwargs)

        # set resources depending on deployment type
        if deploy_env == "prod":
            resource_removal_policy = core.RemovalPolicy.RETAIN
            batch_compute_env_instance_types = [
                aws_ec2.InstanceType("c5.xlarge"),
                aws_ec2.InstanceType("c5.2xlarge"),
                aws_ec2.InstanceType("c5.4xlarge"),
                aws_ec2.InstanceType("c5.9xlarge"),
            ]
            batch_job_definition_memory_limit = 3900
        else:
            resource_removal_policy = core.RemovalPolicy.DESTROY
            batch_compute_env_instance_types = [
                aws_ec2.InstanceType("m5.large"),
                aws_ec2.InstanceType("m5.xlarge"),
            ]
            batch_job_definition_memory_limit = 500

        ############################################################################################
        # PROCESSING ASSETS TABLE
        assets_table = self._create_assets_table(resource_removal_policy)

        ############################################################################################
        # BATCH JOB DEPENDENCIES
        batch_job_queue = self._create_batch_job_queue(
            vpc, batch_compute_env_instance_types, assets_table
        )

        s3_read_only_access_policy = aws_iam.ManagedPolicy.from_aws_managed_policy_name(
            "AmazonS3ReadOnlyAccess"
        )
        batch_job_role = aws_iam.Role(
            self,
            "batch-job-role",
            assumed_by=aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[s3_read_only_access_policy],
        )

        ############################################################################################
        # STATE MACHINE TASKS
        check_stac_metadata_task = self._create_check_stack_metadata_task(
            batch_job_definition_memory_limit, batch_job_role, batch_job_queue
        )
        content_iterator_task = self._create_content_iterator_task(assets_table)
        check_files_checksums_task = self._create_check_files_checksums_task(
            batch_job_definition_memory_limit, batch_job_role, batch_job_queue
        )
        validation_summary_task = self._create_validation_summary_task()
        validation_failure_task = self._create_validation_failure_task()
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

    def _create_assets_table(
        self, resource_removal_policy: core.RemovalPolicy
    ) -> aws_dynamodb.Table:
        assets_table = aws_dynamodb.Table(
            self,
            "processing-assets",
            partition_key=aws_dynamodb.Attribute(name="pk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            point_in_time_recovery=True,
            removal_policy=resource_removal_policy,
        )
        core.Tags.of(assets_table).add("ApplicationLayer", "data-processing")
        return assets_table

    def _create_batch_job_queue(
        self,
        vpc: aws_ec2.IVpc,
        batch_compute_env_instance_types: List[aws_ec2.InstanceType],
        assets_table: aws_dynamodb.Table,
    ) -> aws_batch.JobQueue:
        ec2_policy = aws_iam.ManagedPolicy.from_aws_managed_policy_name(
            "service-role/AmazonEC2ContainerServiceforEC2Role"
        )

        batch_instance_role = aws_iam.Role(
            self,
            "batch-instance-role",
            assumed_by=aws_iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[ec2_policy],
        )
        assets_table.grant_read_write_data(batch_instance_role)

        batch_instance_profile = aws_iam.CfnInstanceProfile(
            self,
            "batch-instance-profile",
            roles=[batch_instance_role.role_name],
        )

        batch_launch_template_data = textwrap.dedent(
            """
            MIME-Version: 1.0
            Content-Type: multipart/mixed; boundary="==MYBOUNDARY=="

            --==MYBOUNDARY==
            Content-Type: text/x-shellscript; charset="us-ascii"

            #!/bin/bash
            echo ECS_IMAGE_PULL_BEHAVIOR=prefer-cached >> /etc/ecs/ecs.config

            --==MYBOUNDARY==--
            """
        )
        launch_template_data = aws_ec2.CfnLaunchTemplate.LaunchTemplateDataProperty(
            user_data=core.Fn.base64(batch_launch_template_data.strip())
        )
        cloudformation_launch_template = aws_ec2.CfnLaunchTemplate(
            self,
            "batch-launch-template",
            launch_template_name="datalake-batch-launch-template",
            launch_template_data=launch_template_data,
        )
        launch_template = aws_batch.LaunchTemplateSpecification(
            launch_template_name=cloudformation_launch_template.launch_template_name
        )

        compute_resources = aws_batch.ComputeResources(
            vpc=vpc,
            minv_cpus=0,
            desiredv_cpus=0,
            maxv_cpus=1000,
            instance_types=batch_compute_env_instance_types,
            instance_role=batch_instance_profile.instance_profile_name,
            allocation_strategy=aws_batch.AllocationStrategy("BEST_FIT_PROGRESSIVE"),
            launch_template=launch_template,
        )
        batch_service_policy = aws_iam.ManagedPolicy.from_aws_managed_policy_name(
            "service-role/AWSBatchServiceRole"
        )
        service_role = aws_iam.Role(
            self,
            "batch-service-role",
            assumed_by=aws_iam.ServicePrincipal("batch.amazonaws.com"),
            managed_policies=[batch_service_policy],
        )
        compute_environment = aws_batch.ComputeEnvironment(
            self,
            "compute-environment",
            compute_resources=compute_resources,
            service_role=service_role,
        )
        return aws_batch.JobQueue(
            self,
            "dataset-version-creation-queue",
            compute_environments=[
                aws_batch.JobQueueComputeEnvironment(
                    compute_environment=compute_environment, order=10
                ),
            ],
            priority=10,
        )

    def _create_check_stack_metadata_task(
        self,
        batch_job_definition_memory_limit: int,
        batch_job_role: aws_iam.Role,
        batch_job_queue: aws_batch.JobQueue,
    ) -> aws_stepfunctions_tasks.BatchSubmitJob:
        name = "check_stac_metadata"
        job_definition = self._create_job_definition(
            name, batch_job_definition_memory_limit, batch_job_role
        )
        container_overrides = aws_stepfunctions_tasks.BatchContainerOverrides(
            command=["--metadata-url", "Ref::metadata_url"]
        )
        payload = aws_stepfunctions.TaskInput.from_object(
            {
                "dataset_id.$": "$.dataset_id",
                "version_id.$": "$.version_id",
                "type.$": "$.type",
                "metadata_url.$": "$.metadata_url",
            }
        )
        return aws_stepfunctions_tasks.BatchSubmitJob(
            self,
            name,
            job_name=f"{name}-job",
            job_definition=job_definition,
            job_queue=batch_job_queue,
            result_path=aws_stepfunctions.JsonPath.DISCARD,
            container_overrides=container_overrides,
            payload=payload,
        )

    def _create_content_iterator_task(
        self, assets_table: aws_dynamodb.Table
    ) -> aws_stepfunctions_tasks.LambdaInvoke:
        name = "content_iterator"
        lambda_function = self._create_lambda_function(name)
        assets_table.grant_read_data(lambda_function)
        return self._create_lambda_invoke(lambda_function, name, "$.content")

    def _create_check_files_checksums_task(
        self,
        batch_job_definition_memory_limit: int,
        batch_job_role: aws_iam.Role,
        batch_job_queue: aws_batch.JobQueue,
    ) -> aws_stepfunctions_tasks.BatchSubmitJob:
        name = "check_files_checksums"
        job_definition = self._create_job_definition(
            name, batch_job_definition_memory_limit, batch_job_role
        )
        container_overrides = aws_stepfunctions_tasks.BatchContainerOverrides(
            command=["--metadata-url", "Ref::metadata_url"],
            environment={"BATCH_JOB_FIRST_ITEM_INDEX": "Ref::first_item"},
        )
        payload = aws_stepfunctions.TaskInput.from_object(
            {
                "dataset_id.$": "$.dataset_id",
                "version_id.$": "$.version_id",
                "type.$": "$.type",
                "metadata_url.$": "$.metadata_url",
                "first_item.$": "$.content.first_item",
            }
        )
        return aws_stepfunctions_tasks.BatchSubmitJob(
            self,
            name,
            job_name=f"{name}{JOB_DEFINITION_SUFFIX}",
            job_definition=job_definition,
            job_queue=batch_job_queue,
            array_size=aws_stepfunctions.JsonPath.number_at("$.content.iteration_size"),
            result_path=aws_stepfunctions.JsonPath.DISCARD,
            container_overrides=container_overrides,
            payload=payload,
        )

    def _create_validation_summary_task(self) -> aws_stepfunctions_tasks.LambdaInvoke:
        name = "validation_summary"
        lambda_function = self._create_lambda_function(name)
        return self._create_lambda_invoke(lambda_function, name, "$.validation")

    def _create_validation_failure_task(self) -> aws_stepfunctions_tasks.LambdaInvoke:
        name = "validation_failure"
        lambda_function = self._create_lambda_function(name)
        return self._create_lambda_invoke(
            lambda_function,
            name,
            aws_stepfunctions.JsonPath.DISCARD,
        )

    def _create_lambda_function(self, task_name: str) -> aws_lambda.Function:
        bundling_options = core.BundlingOptions(
            # pylint:disable=no-member
            image=aws_lambda.Runtime.PYTHON_3_8.bundling_docker_image,
            command=["datalake/backend/bundle.bash", f"processing/{task_name}"],
        )
        lambda_code = aws_lambda.Code.from_asset(path=".", bundling=bundling_options)
        lambda_function = aws_lambda.Function(
            self,
            f"{task_name}-function",
            handler=f"processing.{task_name}.task.lambda_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            code=lambda_code,
        )

        core.Tags.of(lambda_function).add("ApplicationLayer", "data-processing")

        return lambda_function

    def _create_lambda_invoke(
        self, lambda_function: aws_lambda.Function, task_name: str, result_path: str
    ) -> aws_stepfunctions_tasks.LambdaInvoke:
        return aws_stepfunctions_tasks.LambdaInvoke(
            self,
            task_name,
            lambda_function=lambda_function,
            result_path=result_path,
            payload_response_only=True,
        )

    def _create_job_definition(
        self, task_name: str, batch_job_definition_memory_limit: int, batch_job_role: aws_iam.Role
    ) -> aws_batch.JobDefinition:
        image = aws_ecs.ContainerImage.from_asset(
            directory=".",
            file=f"datalake/backend/processing/{task_name}/Dockerfile",
        )
        container = aws_batch.JobDefinitionContainer(
            image=image,
            job_role=batch_job_role,
            memory_limit_mib=batch_job_definition_memory_limit,
            vcpus=1,
        )
        return aws_batch.JobDefinition(
            self,
            f"{task_name}{JOB_DEFINITION_SUFFIX}",
            container=container,
            retry_attempts=4,
        )
