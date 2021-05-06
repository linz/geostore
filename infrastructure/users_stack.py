from os import environ
from typing import Any

from aws_cdk import aws_iam
from aws_cdk.core import Construct, NestedStack, Tags

from backend.resources import ResourceName

from .roles import MAX_SESSION_DURATION


class UsersStack(NestedStack):
    def __init__(self, scope: Construct, stack_id: str, **kwargs: Any) -> None:
        super().__init__(scope, stack_id, **kwargs)

        if saml_provider_arn := environ.get("DATALAKE_SAML_IDENTITY_PROVIDER_ARN"):
            principal = aws_iam.FederatedPrincipal(
                federated=saml_provider_arn,
                assume_role_action="sts:AssumeRoleWithSAML",
                conditions={"StringEquals": {"SAML:aud": "https://signin.aws.amazon.com/saml"}},
            )

        else:
            principal = aws_iam.AccountPrincipal(  # type: ignore[assignment]
                account_id=aws_iam.AccountRootPrincipal().account_id
            )

        self.api_users_role = aws_iam.Role(
            self,
            "api-users-role",
            role_name=ResourceName.API_USERS_ROLE_NAME.value,
            assumed_by=principal,  # type: ignore[arg-type]
            max_session_duration=MAX_SESSION_DURATION,
        )

        self.s3_users_role = aws_iam.Role(
            self,
            "s3-users-role",
            role_name=ResourceName.S3_USERS_ROLE_NAME.value,
            assumed_by=principal,  # type: ignore[arg-type]
            max_session_duration=MAX_SESSION_DURATION,
        )

        Tags.of(self).add("ApplicationLayer", "users")  # type: ignore[arg-type]
