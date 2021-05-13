import os

from aws_cdk.core import RemovalPolicy

if os.environ.get("RESOURCE_REMOVAL_POLICY", "DESTROY").upper() == "RETAIN":
    REMOVAL_POLICY = RemovalPolicy.RETAIN

else:
    REMOVAL_POLICY = RemovalPolicy.DESTROY
