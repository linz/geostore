from os.path import join
from subprocess import check_call
from tempfile import mkdtemp

from aws_cdk import aws_lambda

from .backend import BACKEND_DIRECTORY


def bundled_code(directory: str) -> aws_lambda.Code:
    asset_root = mkdtemp()
    check_call([join(BACKEND_DIRECTORY, "bundle.bash"), directory, asset_root])
    return aws_lambda.Code.from_asset(path=asset_root)
