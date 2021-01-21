"""
Data Lake processing stack.
"""
import textwrap

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
    def __init__(self, scope: core.Construct, stack_id: str, deploy_env, vpc, **kwargs) -> None:
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
        # ### PROCESSING ASSETS STORAGE TABLE ######################################################
        ############################################################################################
        assets_table = aws_dynamodb.Table(
            self,
            "processing-assets",
            partition_key=aws_dynamodb.Attribute(name="pk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            point_in_time_recovery=True,
            removal_policy=resource_removal_policy,
        )

        core.Tags.of(assets_table).add("ApplicationLayer", "data-processing")

        ############################################################################################
        # ### PROCESSING STATE MACHINE #############################################################
        ############################################################################################

        # STATE MACHINE TASKS CONFIGURATION
        content_iterator_name = "content_iterator"
        content_iterator_lambda_function = self._create_lambda_function(content_iterator_name)
        assets_table.grant_read_data(content_iterator_lambda_function)
        content_iterator_lambda_invocation = self._create_lambda_invoke(
            content_iterator_lambda_function, content_iterator_name, "$.content"
        )

        validation_summary_name = "validation_summary"
        validation_summary_lambda_function = self._create_lambda_function(validation_summary_name)
        validation_summary_lambda_invocation = self._create_lambda_invoke(
            validation_summary_lambda_function, validation_summary_name, "$.validation"
        )

        validation_failure_name = "validation_failure"
        validation_failure_lambda_function = self._create_lambda_function(validation_failure_name)
        validation_failure_lambda_invocation = self._create_lambda_invoke(
            validation_failure_lambda_function,
            validation_failure_name,
            aws_stepfunctions.JsonPath.DISCARD,
        )

        # Batch jobs
        s3_read_only_access_policy = aws_iam.ManagedPolicy.from_aws_managed_policy_name(
            "AmazonS3ReadOnlyAccess"
        )
        batch_job_role = aws_iam.Role(
            self,
            "batch-job-role",
            assumed_by=aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[s3_read_only_access_policy],
        )

        batch_service_policy = aws_iam.ManagedPolicy.from_aws_managed_policy_name(
            "service-role/AWSBatchServiceRole"
        )
        batch_service_role = aws_iam.Role(
            self,
            "batch-service-role",
            assumed_by=aws_iam.ServicePrincipal("batch.amazonaws.com"),
            managed_policies=[batch_service_policy],
        )

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
        batch_launch_template = aws_ec2.CfnLaunchTemplate(
            self,
            "batch-launch-template",
            launch_template_name="datalake-batch-launch-template",
            launch_template_data=launch_template_data,
        )

        launch_template = aws_batch.LaunchTemplateSpecification(
            launch_template_name=batch_launch_template.launch_template_name
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
        batch_compute_environment = aws_batch.ComputeEnvironment(
            self,
            "compute-environment",
            compute_resources=compute_resources,
            service_role=batch_service_role,
        )

        batch_job_queue = aws_batch.JobQueue(
            self,
            "dataset-version-creation-queue",
            compute_environments=[
                aws_batch.JobQueueComputeEnvironment(
                    compute_environment=batch_compute_environment, order=10
                ),
            ],
            priority=10,
        )

        check_files_checksums_name = "check_files_checksums"
        check_files_checksums_job_definition = self._create_job_definition(
            check_files_checksums_name, batch_job_role, batch_job_definition_memory_limit
        )
        check_files_checksums_container_overrides = aws_stepfunctions_tasks.BatchContainerOverrides(
            command=["--metadata-url", "Ref::metadata_url"],
            environment={"BATCH_JOB_FIRST_ITEM_INDEX": "Ref::first_item"},
        )
        check_files_checksums_payload = aws_stepfunctions.TaskInput.from_object(
            {
                "dataset_id.$": "$.dataset_id",
                "version_id.$": "$.version_id",
                "type.$": "$.type",
                "metadata_url.$": "$.metadata_url",
                "first_item.$": "$.content.first_item",
            }
        )
        check_files_checksums_batch_submit_job = aws_stepfunctions_tasks.BatchSubmitJob(
            self,
            check_files_checksums_name,
            job_name=f"{check_files_checksums_name}{JOB_DEFINITION_SUFFIX}",
            job_definition=check_files_checksums_job_definition,
            job_queue=batch_job_queue,
            array_size=aws_stepfunctions.JsonPath.number_at("$.content.iteration_size"),
            result_path=aws_stepfunctions.JsonPath.DISCARD,
            container_overrides=check_files_checksums_container_overrides,
            payload=check_files_checksums_payload,
        )

        check_stac_metadata_name = "check_stac_metadata"
        check_stac_metadata_job_definition = self._create_job_definition(
            check_stac_metadata_name, batch_job_role, batch_job_definition_memory_limit
        )
        check_stac_metadata_container_overrides = aws_stepfunctions_tasks.BatchContainerOverrides(
            command=["--metadata-url", "Ref::metadata_url"]
        )
        check_stac_metadata_payload = aws_stepfunctions.TaskInput.from_object(
            {
                "dataset_id.$": "$.dataset_id",
                "version_id.$": "$.version_id",
                "type.$": "$.type",
                "metadata_url.$": "$.metadata_url",
            }
        )
        check_stac_metadata_batch_submit_job = aws_stepfunctions_tasks.BatchSubmitJob(
            self,
            check_stac_metadata_name,
            job_name=f"{check_stac_metadata_name}-job",
            job_definition=check_stac_metadata_job_definition,
            job_queue=batch_job_queue,
            result_path=aws_stepfunctions.JsonPath.DISCARD,
            container_overrides=check_stac_metadata_container_overrides,
            payload=check_stac_metadata_payload,
        )

        # success task
        success_task = aws_stepfunctions.Succeed(self, "success")

        # STATE MACHINE
        # state machine definition
        dataset_version_creation_definition = (
            check_stac_metadata_batch_submit_job.next(content_iterator_lambda_invocation)
            .next(check_files_checksums_batch_submit_job)
            .next(
                aws_stepfunctions.Choice(self, "content_iteration_finished")
                .when(
                    aws_stepfunctions.Condition.not_(
                        aws_stepfunctions.Condition.number_equals("$.content.next_item", -1)
                    ),
                    content_iterator_lambda_invocation,
                )
                .otherwise(
                    validation_summary_lambda_invocation.next(
                        aws_stepfunctions.Choice(self, "validation_successful")
                        .when(
                            aws_stepfunctions.Condition.boolean_equals(
                                "$.validation.success", True
                            ),
                            success_task,
                        )
                        .otherwise(validation_failure_lambda_invocation)
                    ),
                )
            )
        )

        # state machine
        aws_stepfunctions.StateMachine(
            self,
            "dataset-version-creation",
            definition=dataset_version_creation_definition,
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
        self, task_name: str, batch_job_role: aws_iam.Role, batch_job_definition_memory_limit: int
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
