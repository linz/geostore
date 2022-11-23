from aws_cdk import aws_iam

# https://docs.aws.amazon.com/kms/latest/developerguide/key-policy-default.html
AWS_MANAGED_KEY_POLICY = aws_iam.PolicyDocument(
    statements=[
        aws_iam.PolicyStatement(
            actions=[
                "kms:Create*",
                "kms:Describe*",
                "kms:Enable*",
                "kms:List*",
                "kms:Put*",
                "kms:Revoke*",
                "kms:Disable*",
                "kms:Get*",
                "kms:Delete*",
            ],
            principals=[aws_iam.AccountRootPrincipal()],
            resources=["*"],
        )
    ]
)
