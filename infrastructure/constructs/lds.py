from aws_cdk import aws_iam, aws_s3
from aws_cdk.core import Construct, Tags

from geostore.environment import PRODUCTION_ENVIRONMENT_NAME

from .roles import MAX_SESSION_DURATION


class LDS(Construct):
    def __init__(
        self, scope: Construct, stack_id: str, *, env_name: str, storage_bucket: aws_s3.Bucket
    ) -> None:
        super().__init__(scope, stack_id)

        account_principal = aws_iam.AccountPrincipal(account_id=276514628126)
        external_id = {PRODUCTION_ENVIRONMENT_NAME: "koordinates-jAddR"}.get(
            env_name, "koordinates-4BnJQ"
        )
        role = aws_iam.Role(
            self,
            "koordinates-read-role",
            role_name=f"koordinates-s3-access-read-{env_name}",
            assumed_by=account_principal,  # type: ignore[arg-type]
            external_id=external_id,
            max_session_duration=MAX_SESSION_DURATION,
        )
        storage_bucket.grant_read(role)  # type: ignore[arg-type]

        Tags.of(self).add("ApplicationLayer", "lds")  # type: ignore[arg-type]
