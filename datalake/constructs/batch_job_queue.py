import textwrap

from aws_cdk import aws_batch, aws_dynamodb, aws_ec2, aws_iam, core


class BatchJobQueue(core.Construct):
    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        *,
        deploy_env: str,
        processing_assets_table: aws_dynamodb.Table,
        vpc: aws_ec2.IVpc,
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
            assumed_by=aws_iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[ec2_policy],
        )
        processing_assets_table.grant_read_write_data(batch_instance_role)

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
            launch_template_name=f"{deploy_env}-datalake-batch-launch-template",
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
            assumed_by=aws_iam.ServicePrincipal("batch.amazonaws.com"),
            managed_policies=[batch_service_policy],
        )
        compute_environment = aws_batch.ComputeEnvironment(
            self,
            "compute-environment",
            compute_resources=compute_resources,
            service_role=service_role,
        )

        self.job_queue = aws_batch.JobQueue(
            scope,
            f"{construct_id} job queue",
            compute_environments=[
                aws_batch.JobQueueComputeEnvironment(
                    compute_environment=compute_environment, order=10
                ),
            ],
            priority=10,
        )
