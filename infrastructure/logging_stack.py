import constructs
from aws_cdk import aws_cloudtrail, aws_logs, aws_s3, custom_resources
from aws_cdk.aws_s3 import LifecycleRule
from aws_cdk.core import Duration, Stack

from geostore.environment import is_production


class Logging(Stack):
    def __init__(self, scope: constructs.Construct, stack_id: str) -> None:
        super().__init__(scope, stack_id)

        if is_production():
            lifecycle_rules = None
        else:
            lifecycle_rules = [LifecycleRule(expiration=Duration.days(7))]

        trail_bucket = aws_s3.Bucket(
            self,
            "geostore-cloudtrail",
            access_control=aws_s3.BucketAccessControl.PRIVATE,
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=lifecycle_rules,
        )

        api_user_log_group = aws_logs.LogGroup(
            self, "api-user-log", log_group_name="geostore-api-cloudtrail"
        )
        trail = aws_cloudtrail.Trail(
            self,
            "geostore",
            send_to_cloud_watch_logs=True,
            bucket=trail_bucket,  # type: ignore[arg-type]
            cloud_watch_log_group=api_user_log_group,  # type: ignore[arg-type]
        )

        # TODO: Simplify if <https://github.com/aws/aws-cdk/issues/19398> is implemented. pylint:disable=fixme
        endpoint_selectors_call_id = custom_resources.PhysicalResourceId.of(
            "endpoint-function-selectors"
        )
        common_selectors = [
            {"Field": "eventCategory", "Equals": ["Data"]},
            {"Field": "resources.type", "Equals": ["AWS::Lambda::Function"]},
        ]
        endpoint_selectors_call = custom_resources.AwsSdkCall(
            service="CloudTrail",
            action="putEventSelectors",
            parameters={
                "TrailName": trail.trail_arn,
                "AdvancedEventSelectors": [
                    {
                        "Name": "Log 'dataset-versions' Lambda functions",
                        "FieldSelectors": [
                            *common_selectors,
                            {"Field": "resources.ARN", "EndsWith": ["dataset-versions"]},
                        ],
                    },
                    {
                        "Name": "Log 'datasets' Lambda functions",
                        "FieldSelectors": [
                            *common_selectors,
                            {"Field": "resources.ARN", "EndsWith": ["datasets"]},
                        ],
                    },
                    {
                        "Name": "Log 'import-status' Lambda functions",
                        "FieldSelectors": [
                            *common_selectors,
                            {"Field": "resources.ARN", "EndsWith": ["import-status"]},
                        ],
                    },
                ],
            },
            physical_resource_id=endpoint_selectors_call_id,
        )
        endpoint_selectors_policy = custom_resources.AwsCustomResourcePolicy.from_sdk_calls(
            resources=[trail.trail_arn]
        )
        custom_resources.AwsCustomResource(
            self,
            "endpoint-function-selectors",
            on_create=endpoint_selectors_call,
            policy=endpoint_selectors_policy,
        )
