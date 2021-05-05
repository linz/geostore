from os.path import join

from aws_cdk import aws_lambda
from aws_cdk.core import BundlingOptions

from ..runtime import PYTHON_RUNTIME
from .backend import BACKEND_DIRECTORY


def bundled_code(directory: str) -> aws_lambda.Code:
    bundling_options = BundlingOptions(
        image=PYTHON_RUNTIME.bundling_docker_image,  # pylint:disable=no-member
        command=[join(BACKEND_DIRECTORY, "bundle.bash"), directory],
    )
    return aws_lambda.Code.from_asset(path=".", bundling=bundling_options)
