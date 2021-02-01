"""
Data Lake networking stack.
"""
from aws_cdk import aws_ec2, core
from aws_cdk.core import Tags

APPLICATION_NAME_TAG_NAME = "ApplicationName"
APPLICATION_NAME = "geospatial-data-lake"


class NetworkingStack(core.Stack):
    """Data Lake networking stack definition."""

    def __init__(
        self, scope: core.Construct, stack_id: str, deploy_env, use_existing_vpc, **kwargs
    ) -> None:
        super().__init__(scope, stack_id, **kwargs)

        ############################################################################################
        # ### NETWORKING ###########################################################################
        ############################################################################################

        if use_existing_vpc:
            # use existing VPC
            self.datalake_vpc = aws_ec2.Vpc.from_lookup(
                self,
                "datalake-vpc",
                tags={
                    APPLICATION_NAME_TAG_NAME: APPLICATION_NAME,
                    "ApplicationLayer": "networking",
                },
            )

        else:
            # create new VPC
            self.datalake_vpc = aws_ec2.Vpc(
                self,
                "datalake",
                # cidr='10.0.0.0/16',  # TODO: use specific CIDR
                subnet_configuration=[
                    aws_ec2.SubnetConfiguration(
                        cidr_mask=27, name="public", subnet_type=aws_ec2.SubnetType.PUBLIC
                    ),
                    aws_ec2.SubnetConfiguration(
                        cidr_mask=20, name="ecs-cluster", subnet_type=aws_ec2.SubnetType.PRIVATE
                    ),
                    aws_ec2.SubnetConfiguration(
                        name="reserved",
                        subnet_type=aws_ec2.SubnetType.PRIVATE,
                        reserved=True,
                    ),
                ],
                max_azs=99 if deploy_env == "prod" else 1,
            )
            Tags.of(self.datalake_vpc).add("ApplicationLayer", "networking")
