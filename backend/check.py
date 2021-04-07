from enum import Enum


class Check(Enum):
    CHECKSUM = "checksum"
    DUPLICATE_OBJECT_KEY = "duplicate asset name"
    JSON_PARSE = "JSON parse"
    JSON_SCHEMA = "JSON schema"
    STAGING_ACCESS = "staging bucket access"
    MULTIPLE_DIRECTORIES = "multiple directories"
    NON_S3_URL = "not an s3 url"
