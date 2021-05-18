from aws_cdk import aws_iam

ALLOW_DESCRIBE_ANY_S3_JOB = aws_iam.PolicyStatement(
    resources=["*"],
    actions=["s3:DescribeJob"],
)
