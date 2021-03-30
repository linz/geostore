from typing import Iterable

import constructs
from aws_cdk import aws_ssm
from aws_cdk.aws_iam import IGrantable


class Parameter(aws_ssm.StringParameter):
    def __init__(
        self,
        scope: constructs.Construct,
        construct_id: str,
        *,
        string_value: str,
        description: str,
        parameter_name: str,
        readers: Iterable[IGrantable],
    ) -> None:
        super().__init__(
            scope,
            construct_id,
            string_value=string_value,
            description=description,
            parameter_name=parameter_name,
        )

        for reader in readers:
            self.grant_read(reader)
