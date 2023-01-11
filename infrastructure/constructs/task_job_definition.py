from os.path import join
from pathlib import Path
from subprocess import check_call

from aws_cdk import aws_batch_alpha, aws_ecs, aws_iam
from constructs import Construct

from geostore.aws_keys import AWS_DEFAULT_REGION_KEY
from geostore.environment import ENV_NAME_VARIABLE_NAME, is_production
from infrastructure.constructs.bundled_code import LambdaPackaging

from .backend import BACKEND_DIRECTORY


class TaskJobDefinition(aws_batch_alpha.JobDefinition):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        directory: str,
        job_role: aws_iam.Role,
    ):
        if is_production():
            batch_job_definition_memory_limit = 3900
        else:
            batch_job_definition_memory_limit = 500

        python_version_path = Path(__file__).parent / "../../.python-version"
        with python_version_path.open() as python_version:
            docker_python_version = python_version.read().rstrip()

        check_call(
            [
                "poetry",
                "export",
                f"--extras={directory}",
                "--without-hashes",
                f"--output={LambdaPackaging.directory}/{directory}.txt",
            ]
        )

        image = aws_ecs.ContainerImage.from_asset(
            directory=".",
            build_args={
                "python_version": docker_python_version,
                "task": directory,
                "packaging": LambdaPackaging.directory,
            },
            file=join(BACKEND_DIRECTORY, "Dockerfile"),
        )

        container = aws_batch_alpha.JobDefinitionContainer(
            image=image,
            job_role=job_role,
            memory_limit_mib=batch_job_definition_memory_limit,
            vcpus=1,
            environment={
                AWS_DEFAULT_REGION_KEY: job_role.stack.region,
                ENV_NAME_VARIABLE_NAME: env_name,
            },
        )

        super().__init__(scope, construct_id, container=container)
