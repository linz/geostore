import os
from typing import Any

from aws_cdk import aws_iam
from aws_cdk.core import Construct, Duration, Stack, Tags

from backend.resources import ResourceName


class UsersStack(Stack):
    def __init__(self, scope: Construct, stack_id: str, **kwargs: Any) -> None:
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
            role_name=ResourceName.USERS_ROLE_NAME.value,
            assumed_by=principals,  # type: ignore[arg-type]
            max_session_duration=Duration.hours(12),
        )

        Tags.of(self.users_role).add("ApplicationLayer", "users")  # type: ignore[arg-type]
