from logging import Logger
from os import environ
from typing import Callable

from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from multihash import FUNCS, decode

from ..api_keys import MESSAGE_KEY
from ..check import Check
from ..error_response_keys import ERROR_KEY
from ..logging_keys import LOG_MESSAGE_VALIDATION_COMPLETE
from ..processing_assets_model import processing_assets_model_with_meta
from ..s3 import CHUNK_SIZE
from ..step_function import Outcome
from ..types import JsonObject
from ..validation_results_model import ValidationResult, ValidationResultFactory

ARRAY_INDEX_VARIABLE_NAME = "AWS_BATCH_JOB_ARRAY_INDEX"


class ChecksumMismatchError(Exception):
    def __init__(self, actual_hex_digest: str):
        super().__init__()

        self.actual_hex_digest = actual_hex_digest


class ChecksumValidator:
    def __init__(
        self,
        processing_assets_table_name: str,
        validation_result_factory: ValidationResultFactory,
        url_reader: Callable[[str], StreamingBody],
        logger: Logger,
    ):
        self.validation_result_factory = validation_result_factory
        self.url_reader = url_reader

        self.logger = logger

        self.processing_assets_model = processing_assets_model_with_meta(
            assets_table_name=processing_assets_table_name
        )

    def log_failure(self, content: JsonObject) -> None:
        self.logger.error(
            LOG_MESSAGE_VALIDATION_COMPLETE, extra={"outcome": Outcome.FAILED, "error": content}
        )

    def validate(self, hash_key: str, range_key: str) -> None:

        try:
            item = self.processing_assets_model.get(hash_key, range_key=range_key)
        except self.processing_assets_model.DoesNotExist:
            self.log_failure(
                {
                    ERROR_KEY: {MESSAGE_KEY: "Item does not exist"},
                    "parameters": {"hash_key": hash_key, "range_key": range_key},
                }
            )
            raise

        try:
            self.validate_url_multihash(item.url, item.multihash)
        except ChecksumMismatchError as error:
            content = {
                MESSAGE_KEY: f"Checksum mismatch: expected {item.multihash[4:]},"
                f" got {error.actual_hex_digest}"
            }
            self.log_failure(content)
            self.validation_result_factory.save(
                item.url, Check.CHECKSUM, ValidationResult.FAILED, details=content
            )
        else:
            self.logger.info(LOG_MESSAGE_VALIDATION_COMPLETE, extra={"outcome": Outcome.PASSED})
            self.validation_result_factory.save(item.url, Check.CHECKSUM, ValidationResult.PASSED)

    def validate_url_multihash(self, url: str, hex_multihash: str) -> None:

        try:
            url_stream = self.url_reader(url)
        except ClientError as error:
            self.validation_result_factory.save(
                url,
                Check.STAGING_ACCESS,
                ValidationResult.FAILED,
                details={MESSAGE_KEY: str(error)},
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
