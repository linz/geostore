from aws_cdk import aws_dynamodb, core


class Table(aws_dynamodb.Table):
    def __init__(
        self, scope: core.Construct, construct_id: str, *, deploy_env: str, application_layer: str
    ):
        if deploy_env == "prod":
            resource_removal_policy = core.RemovalPolicy.RETAIN
        else:
            resource_removal_policy = core.RemovalPolicy.DESTROY

        super().__init__(
            scope,
            construct_id,
            table_name=construct_id,
            partition_key=aws_dynamodb.Attribute(name="pk", type=aws_dynamodb.AttributeType.STRING),
            sort_key=aws_dynamodb.Attribute(name="sk", type=aws_dynamodb.AttributeType.STRING),
            point_in_time_recovery=True,
            removal_policy=resource_removal_policy,
        )

        core.Tags.of(self).add("ApplicationLayer", application_layer)  # type: ignore[arg-type]
