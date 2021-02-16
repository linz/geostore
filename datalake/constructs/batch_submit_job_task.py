from typing import List, Mapping, Optional

from aws_cdk import aws_batch, aws_iam, aws_stepfunctions, aws_stepfunctions_tasks, core

from ..common import LOG_LEVEL
from .task_job_definition import TaskJobDefinition


class BatchSubmitJobTask(core.Construct):
    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        *,
        deploy_env: str,
        directory: str,
        s3_policy: aws_iam.IManagedPolicy,
        job_queue: aws_batch.JobQueue,
        payload_object: Mapping[str, str],
        container_overrides_command: List[str],
        container_overrides_environment: Optional[Mapping[str, str]] = None,
        array_size: Optional[int] = None,
    ):
        super().__init__(scope, construct_id)

        self.job_role = aws_iam.Role(
            self,
            f"{construct_id}_batch_job_role",
            assumed_by=aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[s3_policy],
        )

        self.job_definition = TaskJobDefinition(
            self,
            f"{construct_id}_task_definition",
            deploy_env=deploy_env,
            directory=directory,
            job_role=self.job_role,
        )

        environment = {"LOGLEVEL": LOG_LEVEL}
        if container_overrides_environment is not None:
            environment.update(container_overrides_environment)

        container_overrides = aws_stepfunctions_tasks.BatchContainerOverrides(
            command=container_overrides_command,
            environment=environment,
        )
        payload = aws_stepfunctions.TaskInput.from_object(payload_object)
        self.batch_submit_job = aws_stepfunctions_tasks.BatchSubmitJob(
            scope,
            f"{construct_id}_batch_submit_job",
            job_name=f"{construct_id}_job",
            job_definition=self.job_definition,
            job_queue=job_queue,
            array_size=array_size,
            result_path=aws_stepfunctions.JsonPath.DISCARD,
            container_overrides=container_overrides,
            payload=payload,
        )
