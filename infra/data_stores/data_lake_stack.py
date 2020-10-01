"""
Data Lake AWS resources definitions.
"""

from aws_cdk import core


class DataLakeStack(core.Stack):
    """Data Lake stack definition."""

    # pylint: disable=redefined-builtin
    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

    # pylint: enable=redefined-builtin
