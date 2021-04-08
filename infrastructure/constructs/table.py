from typing import Optional

from aws_cdk import aws_dynamodb, aws_ssm
from aws_cdk.core import Construct, RemovalPolicy, Tags

from backend.parameter_store import ParameterName


class Table(aws_dynamodb.Table):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        deploy_env: str,
        application_layer: str,
        parameter_name: ParameterName,
        sort_key: Optional[aws_dynamodb.Attribute] = None,
    ):
        if deploy_env == "prod":
            resource_removal_policy = RemovalPolicy.RETAIN
        else:
            resource_removal_policy = RemovalPolicy.DESTROY

        super().__init__(
            scope,
            construct_id,
            partition_key=aws_dynamodb.Attribute(name="pk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=sort_key,
            point_in_time_recovery=True,
            removal_policy=resource_removal_policy,
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        Tags.of(self).add("ApplicationLayer", application_layer)  # type: ignore[arg-type]

        self.name_parameter = aws_ssm.StringParameter(
            self,
            f"{construct_id} table name for {deploy_env}",
            string_value=self.table_name,
            parameter_name=parameter_name.value,
        )
