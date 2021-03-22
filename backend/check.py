from enum import Enum


class Check(Enum):
    CHECKSUM = "checksum"
    JSON_SCHEMA = "JSON schema"
    STAGING_ACCESS = "staging bucket access"
