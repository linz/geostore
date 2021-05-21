from urllib.parse import urlparse

S3_SCHEMA = "s3"
S3_URL_PREFIX = f"{S3_SCHEMA}://"


def s3_url_to_key(url: str) -> str:
    return urlparse(url).path[1:]
