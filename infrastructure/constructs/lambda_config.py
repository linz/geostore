from aws_cdk import aws_lambda
from aws_cdk.core import Duration

LAMBDA_TIMEOUT = Duration.seconds(60)
PYTHON_RUNTIME = aws_lambda.Runtime.PYTHON_3_8
