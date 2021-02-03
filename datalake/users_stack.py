import os
from typing import Any

from aws_cdk import aws_iam, core
from aws_cdk.core import Duration, Tags


class UsersStack(core.Stack):
    def __init__(self, scope: core.Construct, stack_id: str, **kwargs: Any) -> None:
        super().__init__(scope, stack_id, **kwargs)

        account_ids = (
            aws_iam.AccountPrincipal(account_id=account_id)
            for account_id in os.environ.get(
                "DATALAKE_USERS_AWS_ACCOUNTS_IDS", aws_iam.AccountRootPrincipal().account_id
            ).split(",")
        )

        principals = aws_iam.CompositePrincipal(*account_ids)

        self.users_role = aws_iam.Role(
            self,
            "users-role",
            assumed_by=principals,
            max_session_duration=Duration.hours(12),
        )

        Tags.of(self.users_role).add("ApplicationLayer", "users")
