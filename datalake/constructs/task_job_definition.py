from aws_cdk import aws_batch, aws_ecs, aws_iam, core


class TaskJobDefinition(aws_batch.JobDefinition):
    def __init__(
        self,
        scope: core.Construct,
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
            file="datalake/backend/processing/Dockerfile",
        )

        container = aws_batch.JobDefinitionContainer(
            image=image,
            job_role=job_role,  # type: ignore[arg-type]
            memory_limit_mib=batch_job_definition_memory_limit,
            vcpus=1,
            environment={"DEPLOY_ENV": deploy_env},
        )

        super().__init__(scope, construct_id, container=container, retry_attempts=4)
