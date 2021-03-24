from enum import Enum


class Check(Enum):
    CHECKSUM = "checksum"
    JSON_SCHEMA = "JSON schema"
    STAGING_ACCESS = "staging bucket access"
    MULTIPLE_DIRECTORIES = "multiple directories"
    NON_S3_URL = "not an s3 url"
