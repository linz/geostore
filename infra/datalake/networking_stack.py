"""
Data Lake networking stack.
"""
from aws_cdk import aws_ec2, core
from aws_cdk.core import Tags


class NetworkingStack(core.Stack):
    """Data Lake networking stack definition."""

    # pylint: disable=redefined-builtin,too-many-locals
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        ############################################################################################
        # ### NETWORKING ###########################################################################
        ############################################################################################

        self.datalake_vpc = aws_ec2.Vpc(
            self,
            "datalake",
            # cidr='10.0.0.0/16',  # TODO: use speciffic CIDR
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
        )
        Tags.of(self.datalake_vpc).add("ApplicationLayer", "networking")
