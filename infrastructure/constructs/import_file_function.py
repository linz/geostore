from aws_cdk import aws_iam, aws_lambda_python
from aws_cdk.core import Construct

from geostore.environment import ENV_NAME_VARIABLE_NAME

from .bundled_lambda_function import BundledLambdaFunction
from .lambda_config import DEFAULT_LAMBDA_MAX_MEMORY_MEGABYTES
from .sts_policy import ALLOW_ASSUME_ANY_ROLE


class ImportFileFunction(BundledLambdaFunction):
    def __init__(
        self,
        scope: Construct,
        *,
        directory: str,
        invoker: aws_iam.Role,
        env_name: str,
        botocore_lambda_layer: aws_lambda_python.PythonLayerVersion,
        max_memory_megabytes: int = DEFAULT_LAMBDA_MAX_MEMORY_MEGABYTES,
    ):
        super().__init__(
            scope,
            directory.replace("_", "-"),
            directory=directory,
            extra_environment={ENV_NAME_VARIABLE_NAME: env_name},
            botocore_lambda_layer=botocore_lambda_layer,
            max_memory_megabytes=max_memory_megabytes,
        )

        self.add_to_role_policy(
            aws_iam.PolicyStatement(
                actions=["s3:GetObject", "s3:GetObjectAcl", "s3:GetObjectTagging", "s3:ListBucket"],
                resources=["*"],
            ),
        )
        self.add_to_role_policy(ALLOW_ASSUME_ANY_ROLE)

        self.grant_invoke(invoker)  # type: ignore[arg-type]
