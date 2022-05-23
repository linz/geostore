from aws_cdk import Tags, aws_iam, aws_s3
from constructs import Construct

from geostore.environment import is_production

from .roles import MAX_SESSION_DURATION


class LDS(Construct):
    def __init__(
        self, scope: Construct, stack_id: str, *, env_name: str, storage_bucket: aws_s3.Bucket
    ) -> None:
        super().__init__(scope, stack_id)

        account_principal = aws_iam.AccountPrincipal(account_id="276514628126")
        if is_production():
            external_id = "koordinates-jAddR"
        else:
            external_id = "koordinates-4BnJQ"
        role = aws_iam.Role(
            self,
            "koordinates-read-role",
            role_name=f"koordinates-s3-access-read-{env_name}",
            assumed_by=account_principal,
            external_ids=[external_id],
            max_session_duration=MAX_SESSION_DURATION,
        )
        storage_bucket.grant_read(role)

        Tags.of(self).add("ApplicationLayer", "lds")
