import textwrap

from aws_cdk import aws_batch, aws_dynamodb, aws_ec2, aws_iam
from aws_cdk.core import Construct, Fn

APPLICATION_NAME_TAG_NAME = "ApplicationName"
APPLICATION_NAME = "geospatial-data-lake"


class BatchJobQueue(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        deploy_env: str,
        processing_assets_table: aws_dynamodb.Table,
    ):
        # pylint: disable=too-many-locals
        super().__init__(scope, construct_id)

        if deploy_env == "prod":
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

        ec2_policy = aws_iam.ManagedPolicy.from_aws_managed_policy_name(
            "service-role/AmazonEC2ContainerServiceforEC2Role"
        )

        batch_instance_role = aws_iam.Role(
            self,
            "batch-instance-role",
            assumed_by=aws_iam.ServicePrincipal("ec2.amazonaws.com"),  # type: ignore[arg-type]
            managed_policies=[ec2_policy],
        )
        processing_assets_table.grant_read_write_data(batch_instance_role)  # type: ignore[arg-type]

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
            user_data=Fn.base64(batch_launch_template_data.strip())
        )
        cloudformation_launch_template = aws_ec2.CfnLaunchTemplate(
            self,
            "batch-launch-template",
            launch_template_name=f"{deploy_env}-datalake-batch-launch-template",
            launch_template_data=launch_template_data,
        )
        assert cloudformation_launch_template.launch_template_name is not None
        launch_template = aws_batch.LaunchTemplateSpecification(
            launch_template_name=cloudformation_launch_template.launch_template_name
        )

        # use existing VPC in LINZ AWS account.
        # VPC with these tags is required to exist in AWS account before being deployed.
        # A VPC will not be deployed by this project.
        vpc = aws_ec2.Vpc.from_lookup(
            self,
            "datalake-vpc",
            tags={
                APPLICATION_NAME_TAG_NAME: APPLICATION_NAME,
                "ApplicationLayer": "networking",
            },
        )

        compute_resources = aws_batch.ComputeResources(
            vpc=vpc,
            minv_cpus=0,
            desiredv_cpus=0,
            maxv_cpus=1000,
            instance_types=instance_types,
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
            assumed_by=aws_iam.ServicePrincipal("batch.amazonaws.com"),  # type: ignore[arg-type]
            managed_policies=[batch_service_policy],
        )
        compute_environment = aws_batch.ComputeEnvironment(
            self,
            "compute-environment",
            compute_resources=compute_resources,
            service_role=service_role,  # type: ignore[arg-type]
        )

        self.job_queue = aws_batch.JobQueue(
            scope,
            f"{construct_id} job queue",
            compute_environments=[
                aws_batch.JobQueueComputeEnvironment(
                    compute_environment=compute_environment, order=10  # type: ignore[arg-type]
                ),
            ],
            priority=10,
        )
