from aws_cdk import aws_iam, aws_lambda_python
from aws_cdk.core import Construct

from .bundled_lambda_function import BundledLambdaFunction


class ImportFileFunction(BundledLambdaFunction):
    def __init__(
        self,
        scope: Construct,
        *,
        directory: str,
        invoker: aws_iam.Role,
        deploy_env: str,
        botocore_lambda_layer: aws_lambda_python.PythonLayerVersion,
    ):
        super().__init__(
            scope,
            directory.replace("_", "-"),
            directory=directory,
            extra_environment={"DEPLOY_ENV": deploy_env},
            botocore_lambda_layer=botocore_lambda_layer,
        )

        assert self.role is not None
        self.role.add_to_policy(
            aws_iam.PolicyStatement(
                actions=["s3:GetObject", "s3:GetObjectAcl", "s3:GetObjectTagging", "s3:ListBucket"],
                resources=["*"],
            ),
        )

        self.grant_invoke(invoker)  # type: ignore[arg-type]
