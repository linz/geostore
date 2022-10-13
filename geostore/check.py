from enum import Enum


class Check(Enum):
    ASSETS_IN_DATASET = "assets in dataset"
    CHECKSUM = "checksum"
    DATASET_ID_AND_STAC_ID_NOT_THE_SAME = "dataset ID and STAC ID must be the same"
    DUPLICATE_OBJECT_KEY = "duplicate asset name"
    FILE_NOT_FOUND = "file not found in staging or storage"
    INVALID_STAC_ROOT_TYPE = "root type must be catalog or collection"
    JSON_PARSE = "JSON parse"
    JSON_SCHEMA = "JSON schema"
    NON_S3_URL = "not an s3 url"
    NO_ASSETS_IN_DATASET = "no assets in the dataset"
    SECURITY_CLASSIFICATION = "security classification"
    STAGING_ACCESS = "staging bucket access"
    UNKNOWN_CLIENT_ERROR = "unknown client error"
    UNKNOWN_MULTIHASH_ERROR = "unknown multihash error"
