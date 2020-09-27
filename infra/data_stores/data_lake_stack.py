"""
Data Lake AWS resources definitions.
"""

from aws_cdk import core


class DataLakeStack(core.Stack):
    """Data Lake stack definition."""

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:  # pylint: disable=W0622
        super().__init__(scope, id, **kwargs)

        # The code that defines your stack goes here
