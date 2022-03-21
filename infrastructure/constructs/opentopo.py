from aws_cdk import aws_iam, aws_s3
from aws_cdk.core import Construct, Tags

from .roles import MAX_SESSION_DURATION


class OpenTopography(Construct):
    def __init__(
        self, scope: Construct, stack_id: str, *, env_name: str, storage_bucket: aws_s3.Bucket
    ) -> None:
        super().__init__(scope, stack_id)

        account_principal = aws_iam.AccountPrincipal(account_id="011766770214")
        external_id = "opentopography-bahX0"
        role = aws_iam.Role(
            self,
            "opentopography-read-role",
            role_name=f"opentopography-s3-access-read-{env_name}",
            assumed_by=account_principal,
            external_id=external_id,
            max_session_duration=MAX_SESSION_DURATION,
        )
        storage_bucket.grant_read(role)

        Tags.of(self).add("ApplicationLayer", "opentopography")
