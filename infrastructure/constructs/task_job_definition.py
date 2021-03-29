from typing import TYPE_CHECKING, Any

from aws_cdk import aws_batch, aws_ecs, aws_iam

if TYPE_CHECKING:
    from .batch_submit_job_task import BatchSubmitJobTask
else:
    BatchSubmitJobTask = Any


class TaskJobDefinition(aws_batch.JobDefinition):
    def __init__(
        self,
        scope: BatchSubmitJobTask,
        construct_id: str,
        *,
        deploy_env: str,
        directory: str,
        job_role: aws_iam.Role,
    ):
        if deploy_env == "prod":
            batch_job_definition_memory_limit = 3900
        else:
            batch_job_definition_memory_limit = 500

        image = aws_ecs.ContainerImage.from_asset(
            directory=".",
            build_args={"task": directory},
            file="backend/Dockerfile",
        )

        container = aws_batch.JobDefinitionContainer(
            image=image,
            job_role=job_role,  # type: ignore[arg-type]
            memory_limit_mib=batch_job_definition_memory_limit,
            vcpus=1,
            environment={
                "AWS_DEFAULT_REGION": scope.job_role.stack.region,
                "DEPLOY_ENV": deploy_env,
            },
        )

        super().__init__(scope, construct_id, container=container)
