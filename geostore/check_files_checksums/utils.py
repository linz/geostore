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
from ..s3_utils import GeostoreS3Response
from ..step_function import Outcome
from ..types import JsonObject
from ..validation_results_model import ValidationResult, ValidationResultFactory

ARRAY_INDEX_VARIABLE_NAME = "AWS_BATCH_JOB_ARRAY_INDEX"


class ChecksumUtils:
    def __init__(
        self,
        processing_assets_table_name: str,
        validation_result_factory: ValidationResultFactory,
        url_reader: Callable[[str], GeostoreS3Response],
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

    def run(self, hash_key: str, range_key: str) -> None:

        try:
            processing_item = self.processing_assets_model.get(hash_key, range_key=range_key)
        except self.processing_assets_model.DoesNotExist:
            self.log_failure(
                {
                    ERROR_KEY: {MESSAGE_KEY: "Item does not exist"},
                    "parameters": {"hash_key": hash_key, "range_key": range_key},
                }
            )
            raise

        s3_response = self.get_s3_object(processing_item.url)

        self.validate_url_multihash(
            processing_item.url, processing_item.multihash, s3_response.response
        )

        processing_item.update(
            actions=[
                self.processing_assets_model.exists_in_staging.set(s3_response.file_in_staging)
            ]
        )

    def validate_url_multihash(
        self, url: str, hex_multihash: str, s3_file_object: StreamingBody
    ) -> None:
        checksum_function_code = int(hex_multihash[:2], 16)
        checksum_function = FUNCS[checksum_function_code]

        file_digest = checksum_function()
        for chunk in s3_file_object.iter_chunks(chunk_size=CHUNK_SIZE):
            file_digest.update(chunk)

        if file_digest.digest() != decode(bytes.fromhex(hex_multihash)):
            content = {
                MESSAGE_KEY: f"Checksum mismatch: expected {hex_multihash[4:]},"
                f" got {file_digest.hexdigest()}"
            }
            self.log_failure(content)
            self.validation_result_factory.save(
                url, Check.CHECKSUM, ValidationResult.FAILED, details=content
            )
        else:
            self.logger.info(LOG_MESSAGE_VALIDATION_COMPLETE, extra={"outcome": Outcome.PASSED})
            self.validation_result_factory.save(url, Check.CHECKSUM, ValidationResult.PASSED)

    def get_s3_object(self, url: str) -> GeostoreS3Response:
        try:
            return self.url_reader(url)
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code == "NoSuchKey":
                self.validation_result_factory.save(
                    url,
                    Check.FILE_NOT_FOUND,
                    ValidationResult.FAILED,
                    details={
                        MESSAGE_KEY: f"Could not find asset file '{url}' "
                        f"in staging bucket or in the Geostore."
                    },
                )
            else:
                self.validation_result_factory.save(
                    url,
                    Check.UNKNOWN_CLIENT_ERROR,
                    ValidationResult.FAILED,
                    details={
                        MESSAGE_KEY: (
                            f"Unknown client error fetching '{url}'."
                            f" Client error code: '{error_code}'."
                            f" Client error message: '{error.response['Error']['Message']}'"
                        ),
                    },
                )
            raise


def get_job_offset() -> int:
    return int(environ.get(ARRAY_INDEX_VARIABLE_NAME, 0))
