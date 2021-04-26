from aws_cdk import aws_lambda
from aws_cdk.core import BundlingOptions


def bundled_code(directory: str) -> aws_lambda.Code:
    bundling_options = BundlingOptions(
        image=aws_lambda.Runtime.PYTHON_3_8.bundling_docker_image,  # pylint:disable=no-member
        command=["backend/bundle.bash", directory],
    )
    return aws_lambda.Code.from_asset(path=".", bundling=bundling_options)
