from typing import Optional

from aws_cdk import aws_dynamodb, aws_ssm
from constructs import Construct

from geostore.parameter_store import ParameterName

from .removal_policy import REMOVAL_POLICY


class Table(aws_dynamodb.Table):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        env_name: str,
        parameter_name: ParameterName,
        sort_key: Optional[aws_dynamodb.Attribute] = None,
    ):

        super().__init__(
            scope,
            construct_id,
            partition_key=aws_dynamodb.Attribute(name="pk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=sort_key,
            point_in_time_recovery=True,
            removal_policy=REMOVAL_POLICY,
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        self.name_parameter = aws_ssm.StringParameter(
            self,
            f"{construct_id} table name for {env_name}",
            string_value=self.table_name,
            parameter_name=parameter_name.value,
        )
