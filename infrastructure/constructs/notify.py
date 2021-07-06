from os import environ

from aws_cdk import (
    aws_events,
    aws_events_targets,
    aws_iam,
    aws_lambda_python,
    aws_sns,
    aws_ssm,
    aws_stepfunctions,
)
from aws_cdk.core import Construct

from backend.environment import ENV_NAME_VARIABLE_NAME
from backend.notify_status_update.task import SLACK_URL_ENV_NAME
from backend.parameter_store import ParameterName
from backend.resources import ResourceName

from .bundled_lambda_function import BundledLambdaFunction
from .common import grant_parameter_read_access
from .s3_policy import ALLOW_DESCRIBE_ANY_S3_JOB
from .table import Table


class Notify(Construct):
    def __init__(
        self,
        scope: Construct,
        stack_id: str,
        *,
        botocore_lambda_layer: aws_lambda_python.PythonLayerVersion,
        env_name: str,
        state_machine: aws_stepfunctions.StateMachine,
        validation_results_table: Table,
    ) -> None:
        super().__init__(scope, stack_id)

        slack_notify_function = BundledLambdaFunction(
            scope,
            f"{env_name}-notify-status-update-function",
            directory="notify_status_update",
            extra_environment={
                ENV_NAME_VARIABLE_NAME: env_name,
            },
            botocore_lambda_layer=botocore_lambda_layer,
        )
        if SLACK_URL_ENV_NAME in environ:
            slack_notify_function.add_environment(
                SLACK_URL_ENV_NAME,
                environ[SLACK_URL_ENV_NAME],
            )

            validation_results_table.grant_read_data(slack_notify_function)
            validation_results_table.grant(slack_notify_function, "dynamodb:DescribeTable")
            state_machine.grant_read(slack_notify_function)

            slack_notify_function.add_to_role_policy(ALLOW_DESCRIBE_ANY_S3_JOB)

        # Allow anyone to subscribe to topic
        step_function_topic = aws_sns.Topic(
            scope,
            "geostore-stepfunction-status-topic",
            topic_name=ResourceName.SNS_TOPIC_NAME.value,
        )
        sns_topic_arn_parameter = aws_ssm.StringParameter(
            self,
            "status-sns-topic-arn",
            string_value=step_function_topic.topic_arn,
            description=f"Status SNS Topic ARN for {env_name}",
            parameter_name=ParameterName.STATUS_SNS_TOPIC_ARN.value,
        )

        # Allow access to any validations
        grant_parameter_read_access(
            {
                sns_topic_arn_parameter: [slack_notify_function],
                validation_results_table.name_parameter: [
                    slack_notify_function,
                ],
            }
        )
        step_function_topic.grant_publish(slack_notify_function)

        step_function_topic.add_to_resource_policy(
            aws_iam.PolicyStatement(
                actions=["sns:Subscribe", "sns:Receive"],
                principals=[aws_iam.AnyPrincipal()],  # type: ignore[list-item]
                resources=[step_function_topic.topic_arn],
            )
        )

        aws_events.Rule(
            scope,
            "geostore-cloudwatch-stepfunctions-rule",
            enabled=True,
            rule_name=ResourceName.CLOUDWATCH_RULE_NAME.value,
            description="Cloudwatch rule to detect import status updates",
            event_pattern=aws_events.EventPattern(
                source=["aws.states"],
                detail_type=["Step Functions Execution Status Change"],
                detail={"stateMachineArn": [state_machine.state_machine_arn]},
            ),
            targets=[
                aws_events_targets.LambdaFunction(slack_notify_function)  # type: ignore[list-item]
            ],
        )
