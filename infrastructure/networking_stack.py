from aws_cdk import aws_ec2
from aws_cdk.core import Construct, Stack, Tags

from geostore.environment import is_production


class NetworkingStack(Stack):
    def __init__(self, scope: Construct, stack_id: str) -> None:
        super().__init__(scope, stack_id)

        ############################################################################################
        # ### NETWORKING ###########################################################################
        ############################################################################################

        # create new VPC
        aws_ec2.Vpc(
            self,
            "geostore",
            # cidr='10.0.0.0/16',  # TODO: use specific CIDR pylint:disable=fixme
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
            max_azs=99 if is_production() else 1,
        )

        Tags.of(self).add("ApplicationLayer", "networking")
