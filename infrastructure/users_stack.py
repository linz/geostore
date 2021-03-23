import os
from typing import Any

from aws_cdk import aws_iam
from aws_cdk.core import Construct, Duration, Stack, Tags

from backend.resources import ResourceName


class UsersStack(Stack):
    def __init__(self, scope: Construct, stack_id: str, **kwargs: Any) -> None:
        super().__init__(scope, stack_id, **kwargs)

        saml_provider_arn = os.environ.get("DATALAKE_SAML_IDENTITY_PROVIDER_ARN")

        if saml_provider_arn:
            principal = aws_iam.FederatedPrincipal(
                federated=saml_provider_arn,
                assume_role_action="sts:AssumeRoleWithSAML",
                conditions={"StringEquals": {"SAML:aud": "https://signin.aws.amazon.com/saml"}},
            )

        else:
            principal = aws_iam.AccountPrincipal(  # type: ignore[assignment]
                account_id=aws_iam.AccountRootPrincipal().account_id
            )

        self.users_role = aws_iam.Role(
            self,
            "users-role",
            role_name=ResourceName.USERS_ROLE_NAME.value,
            assumed_by=principal,  # type: ignore[arg-type]
            max_session_duration=Duration.hours(12),
        )
        Tags.of(self.users_role).add("ApplicationLayer", "users")  # type: ignore[arg-type]
