"""
Data Lake processing stack.
"""
import textwrap

from aws_cdk import (
    aws_batch,
    aws_ec2,
    aws_ecs,
    aws_iam,
    aws_lambda,
    aws_stepfunctions,
    aws_stepfunctions_tasks,
    core,
)
from aws_cdk.core import Tags


class ProcessingStack(core.Stack):
    """Data Lake processing stack definition."""

    # pylint: disable=redefined-builtin,too-many-locals
    def __init__(self, scope: core.Construct, id: str, deploy_env, vpc, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        ENV = deploy_env

        ############################################################################################
        # ### DATASET VERSION CREATE ###############################################################
        ############################################################################################

        # STATE MACHINE TASKS CONFIGURATION
        # * type: lambda|batch
        # * parallel: True|False
        # * input_path: "$"
        # * output_path: "$"
        # * result_path: "$"
        # * items_path: "$"
        creation_tasks = {}

        creation_tasks["content_iterator"] = {"type": "lambda", "result_path": "$.content"}

        creation_tasks["validation_summary"] = {"type": "lambda", "result_path": "$.validation"}

        creation_tasks["validation_failure"] = {
            "type": "lambda",
            "result_path": aws_stepfunctions.JsonPath.DISCARD,
        }

        creation_tasks["check_flat_directory_structure"] = {
            "type": "batch",
            "parallel": False,
            "result_path": aws_stepfunctions.JsonPath.DISCARD,
        }

        creation_tasks["check_files_checksums"] = {
            "type": "batch",
            "parallel": True,
            "result_path": aws_stepfunctions.JsonPath.DISCARD,
        }

        # AWS BATCH COMPUTE ENVIRONMENT
        batch_service_role = aws_iam.Role(
            self,
            "batch-service-role",
            assumed_by=aws_iam.ServicePrincipal("batch.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSBatchServiceRole"
                ),
            ],
        )

        batch_instance_role = aws_iam.Role(
            self,
            "batch-instance-role",
            assumed_by=aws_iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonEC2ContainerServiceforEC2Role"
                ),
            ],
        )

        batch_instance_profile = aws_iam.CfnInstanceProfile(
            self,
            "batch-instance-profile",
            roles=[
                batch_instance_role.role_name,
            ],
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

        batch_launch_template = aws_ec2.CfnLaunchTemplate(
            self,
            "batch-launch-template",
            launch_template_name="datalake-batch-launch-template",
            launch_template_data={
                "userData": core.Fn.base64(batch_launch_template_data.strip()),
            },
        )

        if ENV == "prod":
            instance_types = [
                aws_ec2.InstanceType("c5.xlarge"),
                aws_ec2.InstanceType("c5.2xlarge"),
                aws_ec2.InstanceType("c5.4xlarge"),
                aws_ec2.InstanceType("c5.9xlarge"),
            ]
        else:
            instance_types = [
                aws_ec2.InstanceType("m5.large"),
                aws_ec2.InstanceType("m5.xlarge"),
            ]

        batch_compute_environment = aws_batch.ComputeEnvironment(
            self,
            "compute-environment",
            compute_resources=aws_batch.ComputeResources(
                vpc=vpc,
                # vpc_subnets=vpc.select_subnets(subnet_group_name="ecs-cluster"),  # TODO
                minv_cpus=0,
                desiredv_cpus=0,
                maxv_cpus=1000,
                instance_types=instance_types,
                instance_role=batch_instance_profile.instance_profile_name,
                allocation_strategy=aws_batch.AllocationStrategy("BEST_FIT_PROGRESSIVE"),
                launch_template=aws_batch.LaunchTemplateSpecification(
                    launch_template_name=batch_launch_template.launch_template_name
                ),
            ),
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

        # BUNDLING AND TASKS BUILDING
        step_tasks = {}
        for task in creation_tasks:

            # lambda functions
            if creation_tasks[task]["type"] == "lambda":

                lambda_function = aws_lambda.Function(
                    self,
                    f"{task}-function",
                    handler=f"processing.{task}.task.lambda_handler",
                    runtime=aws_lambda.Runtime.PYTHON_3_6,
                    code=aws_lambda.Code.from_asset(
                        path="..",
                        bundling=core.BundlingOptions(
                            # pylint:disable=no-member
                            image=aws_lambda.Runtime.PYTHON_3_6.bundling_docker_image,
                            command=["backend/processing/bundle.bash", f"{task}"],
                        ),
                    ),
                )

                step_tasks[task] = aws_stepfunctions_tasks.LambdaInvoke(
                    self,
                    f"{task}",
                    lambda_function=lambda_function,
                    input_path=creation_tasks[task].get("input_path", "$"),
                    output_path=creation_tasks[task].get("output_path", "$"),
                    result_path=creation_tasks[task].get("result_path", "$"),
                    payload_response_only=True,
                )

                Tags.of(lambda_function).add("ApplicationLayer", "data-processing")

            # aws batch jobs
            if creation_tasks[task]["type"] == "batch":

                job_definition = aws_batch.JobDefinition(
                    self,
                    f"{task}-job",
                    container=aws_batch.JobDefinitionContainer(
                        image=aws_ecs.ContainerImage.from_asset(
                            directory=f"../backend/processing/{task}",
                        ),
                        memory_limit_mib=3900 if ENV == "prod" else 500,
                        vcpus=1,
                    ),
                    retry_attempts=4,
                )

                job_command = [
                    "--dataset-id",
                    "Ref::dataset_id",
                    "--version-id",
                    "Ref::version_id",
                    "--type",
                    "Ref::type",
                    "--metadata-url",
                    "Ref::metadata_url",
                    "--dataset-id",
                ]
                job_environment = {"BATCH_JOB_FIRST_ITEM_INDEX": "Ref::first_item"}

                job_payload_data = {
                    "dataset_id.$": "$.dataset_id",
                    "version_id.$": "$.version_id",
                    "type.$": "$.type",
                    "metadata_url.$": "$.metadata_url",
                }
                job_payload_data_parallel = {"first_item.$": "$.content.first_item"}
                job_payload_single = aws_stepfunctions.TaskInput.from_object(job_payload_data)
                job_payload_parallel = aws_stepfunctions.TaskInput.from_object(
                    {**job_payload_data, **job_payload_data_parallel}
                )

                if creation_tasks[task]["parallel"]:
                    step_tasks[task] = aws_stepfunctions_tasks.BatchSubmitJob(
                        self,
                        f"{task}",
                        job_name=f"{task}-job",
                        job_definition=job_definition,
                        job_queue=batch_job_queue,
                        array_size=aws_stepfunctions.JsonPath.number_at("$.content.iteration_size"),
                        input_path=creation_tasks[task].get("input_path", "$"),
                        output_path=creation_tasks[task].get("output_path", "$"),
                        result_path=creation_tasks[task].get("result_path", "$"),
                        container_overrides=aws_stepfunctions_tasks.BatchContainerOverrides(
                            command=job_command,
                            environment=job_environment,
                        ),
                        payload=job_payload_parallel,
                    )

                else:
                    step_tasks[task] = aws_stepfunctions_tasks.BatchSubmitJob(
                        self,
                        f"{task}",
                        job_name=f"{task}-job",
                        job_definition=job_definition,
                        job_queue=batch_job_queue,
                        input_path=creation_tasks[task].get("input_path", "$"),
                        output_path=creation_tasks[task].get("output_path", "$"),
                        result_path=creation_tasks[task].get("result_path", "$"),
                        container_overrides=aws_stepfunctions_tasks.BatchContainerOverrides(
                            command=job_command,
                        ),
                        payload=job_payload_single,
                    )

        # success task
        step_tasks["success"] = aws_stepfunctions.Succeed(
            self,
            "success",
        )

        # STATE MACHINE
        # state machine definition
        dataset_version_creation_definition = (
            step_tasks["check_flat_directory_structure"]
            .next(step_tasks["content_iterator"])
            .next(step_tasks["check_files_checksums"])
            .next(
                aws_stepfunctions.Choice(self, "content_iteration_finished")
                .when(
                    aws_stepfunctions.Condition.not_(
                        aws_stepfunctions.Condition.number_equals("$.content.next_item", -1)
                    ),
                    step_tasks["content_iterator"],
                )
                .otherwise(
                    step_tasks["validation_summary"].next(
                        aws_stepfunctions.Choice(self, "validation_successful")
                        .when(
                            aws_stepfunctions.Condition.boolean_equals(
                                "$.validation.success", True
                            ),
                            step_tasks["success"],
                        )
                        .otherwise(step_tasks["validation_failure"])
                    ),
                )
            )
        )

        # state machine
        creation_process = aws_stepfunctions.StateMachine(  # pylint:disable=unused-variable
            self,
            "dataset-version-creation",
            definition=dataset_version_creation_definition,
        )
