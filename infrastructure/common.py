from logging import DEBUG, getLevelName
from os.path import dirname
from typing import Iterable, Mapping

from aws_cdk import aws_iam, aws_ssm

LOG_LEVEL = getLevelName(DEBUG)

PROJECT_DIRECTORY = dirname(dirname(__file__))


def grant_parameter_read_access(
    parameter_readers: Mapping[aws_ssm.StringParameter, Iterable[aws_iam.IGrantable]]
) -> None:
    for parameter, readers in parameter_readers.items():
        for reader in readers:
            parameter.grant_read(reader)
