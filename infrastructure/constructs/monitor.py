from os import environ

from aws_cdk import (
    Duration,
    aws_cloudwatch,
    aws_cloudwatch_actions,
    aws_iam,
    aws_lambda_python_alpha,
    aws_sns,
    aws_sqs,
)
from constructs import Construct

from geostore.environment import ENV_NAME_VARIABLE_NAME
from geostore.notify_status_update.task import SLACK_URL_ENV_NAME
from geostore.parameter_store import ParameterName, get_param
from geostore.resources import Resource

from .bundled_lambda_function import BundledLambdaFunction


class Monitor(Construct):
    def __init__(
        self,
        scope: Construct,
        stack_id: str,
        *,
        botocore_lambda_layer: aws_lambda_python_alpha.PythonLayerVersion,
        env_name: str,
    ) -> None:
        super().__init__(scope, stack_id)

        slack_notify_function = BundledLambdaFunction(
            scope,
            "monitor-catalog-queue",
            directory="monitor_catalog_queue",
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

        monitor_catalog_queue_sns_topic = aws_sns.Topic(
            scope,
            "geostore-monitor-catalog-queue-sns-topic",
            topic_name=Resource.SNS_TOPIC_NAME.resource_name,
        )

        monitor_catalog_queue_sns_topic.grant_publish(slack_notify_function)

        monitor_catalog_queue_sns_topic.add_to_resource_policy(
            aws_iam.PolicyStatement(
                actions=["sns:Subscribe", "sns:Receive"],
                principals=[aws_iam.AnyPrincipal()],
                resources=[monitor_catalog_queue_sns_topic.topic_arn],
            )
        )

        update_catalog_queue = aws_sqs.Queue.from_queue_arn(
            scope,
            "update-catalog-queue",
            queue_arn=get_param(ParameterName.UPDATE_CATALOG_MESSAGE_QUEUE_ARN),
        )

        metric = update_catalog_queue.metric_approximate_age_of_oldest_message(
            period=Duration.minutes(60), statistic="Minimum"
        )

        alarm = metric.create_alarm(
            self,
            "monitor-sqs-alarm",
            evaluation_periods=1,
            threshold=0,
            comparison_operator=aws_cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
        )

        alarm.add_alarm_action(aws_cloudwatch_actions.SnsAction(monitor_catalog_queue_sns_topic))
