from aws_cdk import aws_iam

ALLOW_ASSUME_ANY_ROLE = aws_iam.PolicyStatement(actions=["sts:AssumeRole"], resources=["*"])
