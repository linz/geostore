from botocore.config import Config

CONFIG = Config(retries={"max_attempts": 5, "mode": "standard"})
