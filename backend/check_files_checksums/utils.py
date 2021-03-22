from json import dumps
from logging import Logger
from os import environ
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError  # type: ignore[import]
from multihash import FUNCS, decode  # type: ignore[import]

from ..check import Check
from ..processing_assets_model import ProcessingAssetsModel
from ..types import JsonObject
from ..validation_results_model import ValidationResult, ValidationResultFactory

if TYPE_CHECKING:
    # When type checking we want to use the third party package's stub
    from mypy_boto3_s3 import S3Client
else:
    # In production we want to avoid depending on a package which has no runtime impact
    S3Client = object

ARRAY_INDEX_VARIABLE_NAME = "AWS_BATCH_JOB_ARRAY_INDEX"

CHUNK_SIZE = 1024

S3_CLIENT = boto3.client("s3")


class ChecksumMismatchError(Exception):
    def __init__(self, actual_hex_digest: str):
        super().__init__()

        self.actual_hex_digest = actual_hex_digest


class ChecksumValidator:
    def __init__(
        self,
        validation_result_factory: ValidationResultFactory,
        logger: Logger,
    ):
        self.validation_result_factory = validation_result_factory
        self.logger = logger

    def log_failure(self, content: JsonObject) -> None:
        self.logger.error(dumps({"success": False, **content}))

    def validate(self, hash_key: str, range_key: str) -> None:

        try:
            item = ProcessingAssetsModel.get(hash_key, range_key=range_key)
        except ProcessingAssetsModel.DoesNotExist:
            self.log_failure(
                {
                    "error": {"message": "Item does not exist"},
                    "parameters": {"hash_key": hash_key, "range_key": range_key},
                }
            )
            raise

        try:
            self.validate_url_multihash(item.url, item.multihash)
        except ChecksumMismatchError as error:
            content = {
                "message": f"Checksum mismatch: expected {item.multihash[4:]},"
                f" got {error.actual_hex_digest}"
            }
            self.log_failure(content)
            self.validation_result_factory.save(
                item.url, Check.CHECKSUM, ValidationResult.FAILED, details=content
            )
        else:
            self.logger.info(dumps({"success": True, "message": ""}))
            self.validation_result_factory.save(item.url, Check.CHECKSUM, ValidationResult.PASSED)

    def validate_url_multihash(self, url: str, hex_multihash: str) -> None:
        parsed_url = urlparse(url)
        bucket = parsed_url.netloc
        key = parsed_url.path.lstrip("/")
        try:
            url_stream = S3_CLIENT.get_object(Bucket=bucket, Key=key)["Body"]
        except ClientError as error:
            self.validation_result_factory.save(
                url,
                Check.STAGING_ACCESS,
                ValidationResult.FAILED,
                details={"message": str(error)},
            )
            raise

        checksum_function_code = int(hex_multihash[:2], 16)
        checksum_function = FUNCS[checksum_function_code]

        file_digest = checksum_function()
        for chunk in url_stream.iter_chunks(chunk_size=CHUNK_SIZE):
            file_digest.update(chunk)

        if file_digest.digest() != decode(bytes.fromhex(hex_multihash)):
            raise ChecksumMismatchError(file_digest.hexdigest())


def get_job_offset() -> int:
    return int(environ.get(ARRAY_INDEX_VARIABLE_NAME, 0))
