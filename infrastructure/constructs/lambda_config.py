from aws_cdk import aws_lambda
from aws_cdk.core import Duration

PYTHON_RUNTIME = aws_lambda.Runtime.PYTHON_3_8

DEFAULT_LAMBDA_MAX_MEMORY_MEBIBYTES = 1024
DEFAULT_LAMBDA_TIMEOUT = Duration.seconds(60)
