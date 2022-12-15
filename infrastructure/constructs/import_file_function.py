from aws_cdk import Duration, aws_iam
from constructs import Construct

from geostore.environment import ENV_NAME_VARIABLE_NAME

from .bundled_lambda_function import BundledLambdaFunction
from .lambda_config import DEFAULT_LAMBDA_TIMEOUT
from .sts_policy import ALLOW_ASSUME_ANY_ROLE


class ImportFileFunction(BundledLambdaFunction):
    def __init__(
        self,
        scope: Construct,
        *,
        directory: str,
        invoker: aws_iam.Role,
        env_name: str,
        timeout: Duration = DEFAULT_LAMBDA_TIMEOUT,
    ):
        super().__init__(
            scope,
            directory.title().replace("_", ""),
            directory=directory,
            extra_environment={ENV_NAME_VARIABLE_NAME: env_name},
            timeout=timeout,
        )

        self.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["s3:GetObject", "s3:GetObjectAcl", "s3:GetObjectTagging", "s3:ListBucket"],
                resources=["*"],
            ),
        )
        self.add_to_role_policy(ALLOW_ASSUME_ANY_ROLE)

        self.grant_invoke(invoker)
