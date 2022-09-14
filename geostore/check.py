from enum import Enum


class Check(Enum):
    ASSETS_IN_DATASET = "assets in dataset"
    CHECKSUM = "checksum"
    DUPLICATE_OBJECT_KEY = "duplicate asset name"
    FILE_NOT_FOUND = "file not found in staging or storage"
    JSON_PARSE = "JSON parse"
    JSON_SCHEMA = "JSON schema"
    NON_S3_URL = "not an s3 url"
    SECURITY_CLASSIFICATION = "security classification"
    STAGING_ACCESS = "staging bucket access"
    UNKNOWN_CLIENT_ERROR = "unknown client error"
    UNKNOWN_MULTIHASH_ERROR = "unknown multihash error"
    UPLOADED_ASSETS_SHOULD_BE_CATALOG_OR_COLLECTION_ERROR = "assets should be catalog or collection"
    NO_ASSETS_FOUND_ERROR_MESSAGE = "No assets found in dataset"
