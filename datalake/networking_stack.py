"""
Data Lake networking stack.
"""
from aws_cdk import aws_ec2, core

APPLICATION_NAME_TAG_NAME = "ApplicationName"
APPLICATION_NAME = "geospatial-data-lake"


class NetworkingStack(core.Stack):
    """Data Lake networking stack definition."""

    def __init__(self, scope: core.Construct, stack_id: str, **kwargs) -> None:
        super().__init__(scope, stack_id, **kwargs)

        ############################################################################################
        # ### NETWORKING ###########################################################################
        ############################################################################################

        # use existing VPC in LINZ AWS account.
        # VPC with these tags is required to exist in AWS account before being deployed.
        # A VPC will not be deployed by this project.
        self.datalake_vpc = aws_ec2.Vpc.from_lookup(
            self,
            "datalake-vpc",
            tags={
                "ApplicationName": "geospatial-data-lake",
                "ApplicationLayer": "networking",
            },
        )
