from os.path import join

from aws_cdk import BundlingOptions, aws_lambda

from .backend import BACKEND_DIRECTORY
from .lambda_config import PYTHON_RUNTIME


def bundled_code(directory: str) -> aws_lambda.Code:
    bundling_options = BundlingOptions(
        image=PYTHON_RUNTIME.bundling_image,  # pylint:disable=no-member
        command=[join(BACKEND_DIRECTORY, "bundle.bash"), directory],
    )
    return aws_lambda.Code.from_asset(path=".", bundling=bundling_options)
