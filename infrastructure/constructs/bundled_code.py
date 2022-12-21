import atexit
import shutil
import tempfile
from functools import lru_cache
from re import sub
from subprocess import check_call, check_output
from sys import executable
from typing import List

from aws_cdk import BundlingOptions, aws_lambda

from .backend import BACKEND_DIRECTORY
from .lambda_config import PYTHON_RUNTIME


@lru_cache
def lambda_packaging_dir() -> str:
    packaging_dir = tempfile.mkdtemp(dir=BACKEND_DIRECTORY, prefix=".lambda_out_")
    atexit.register(lambda: shutil.rmtree(packaging_dir))
    return packaging_dir


def poetry_export_extras(lambda_directory: str) -> List[str]:
    # There isn't an elegant way of getting poetry to install package dependencies in a bespoke
    # target lambda_directory within Python, so we export a requirements file and install using pip.
    # This has been raised and discussed by the community as below:
    # https://github.com/python-poetry/poetry/issues/1937

    export_extras = check_output(
        ["poetry", "export", f"--extras={lambda_directory}", "--without-hashes"]
    )
    # Remove botocore as this is already installed in the lambda layer
    export_extras = sub(b"botocore==.*\n", b"", export_extras)

    return export_extras.decode("utf-8").splitlines()


def pip_install_requirements(
    lambda_directory: str, packaging_directory: str, export_extras: List[str]
) -> None:
    # Documentation recommend against calling pip internal api; rather, via command line
    # https://pip.pypa.io/en/latest/user_guide/#using-pip-from-your-program

    check_call(
        [
            executable,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--quiet",
            f"--cache-dir={packaging_directory}/cache",
            f"--target={packaging_directory}/{lambda_directory}",
            *export_extras,
        ]
    )


def bundled_code(lambda_directory: str) -> aws_lambda.Code:
    packaging_directory = lambda_packaging_dir()
    export_extras = poetry_export_extras(lambda_directory)
    pip_install_requirements(lambda_directory, packaging_directory, export_extras)
    bundling_options = BundlingOptions(
        image=PYTHON_RUNTIME.bundling_image,  # pylint:disable=no-member
        command=[
            "bash",
            "-c",
            f"""mkdir --parents /asset-output/geostore/{lambda_directory} && \
                cp --archive --update {packaging_directory}/{lambda_directory}/* /asset-output/ && \
                cp --archive --update /asset-input/geostore/*.py /asset-output/geostore/ && \
                cp --archive --update /asset-input/geostore/{lambda_directory} /asset-output/geostore/""",  # pylint: disable=line-too-long
        ],
    )
    return aws_lambda.Code.from_asset(path=".", bundling=bundling_options)
