from functools import lru_cache
from typing import TYPE_CHECKING

import boto3

from .boto3_config import CONFIG

if TYPE_CHECKING:
    from mypy_boto3_sts import STSClient
else:
    STSClient = object  # pragma: no mutate

STS_CLIENT: STSClient = boto3.client("sts", config=CONFIG)


@lru_cache
def get_account_number() -> str:
    caller_identity = STS_CLIENT.get_caller_identity()
    assert "Account" in caller_identity, caller_identity
    return caller_identity["Account"]
