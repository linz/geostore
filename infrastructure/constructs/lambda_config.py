from aws_cdk import Duration, aws_lambda, aws_logs

from geostore.environment import is_production

PYTHON_RUNTIME = aws_lambda.Runtime.PYTHON_3_9

DEFAULT_LAMBDA_MAX_MEMORY_MEBIBYTES = 1024
DEFAULT_LAMBDA_TIMEOUT = Duration.seconds(60)

if is_production():
    RETENTION_DAYS = aws_logs.RetentionDays.ONE_YEAR
else:
    RETENTION_DAYS = aws_logs.RetentionDays.THREE_MONTHS
