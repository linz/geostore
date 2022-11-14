from aws_cdk import Duration, aws_lambda, aws_logs

PYTHON_RUNTIME = aws_lambda.Runtime.PYTHON_3_9
LOG_RETENTION = aws_logs.RetentionDays.THREE_MONTHS

DEFAULT_LAMBDA_MAX_MEMORY_MEBIBYTES = 1024
DEFAULT_LAMBDA_TIMEOUT = Duration.seconds(60)
