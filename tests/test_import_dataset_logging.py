import json
import logging
from json import dumps
from unittest.mock import MagicMock, patch

from jsonschema import ValidationError  # type: ignore[import]
from pytest import mark
from pytest_subtests import SubTests  # type: ignore[import]

from backend.import_dataset.task import lambda_handler

from .aws_utils import ProcessingAsset, any_lambda_context, any_s3_url
from .general_generators import any_etag
from .stac_generators import any_dataset_id, any_dataset_version_id, any_hex_multihash


class TestLogging:
    logger: logging.Logger

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logging.getLogger("backend.import_dataset.task")

    @patch("backend.import_dataset.task.S3CONTROL_CLIENT.create_job")
    @patch("backend.import_dataset.task.S3_CLIENT.head_object")
    @mark.infrastructure
    def should_log_payload(
        self,
        head_object_mock: MagicMock,
        create_job_mock: MagicMock,  # pylint:disable=unused-argument
    ) -> None:
        # Given

        body = {
            "dataset_id": any_dataset_id(),
            "metadata_url": any_s3_url(),
            "version_id": any_dataset_version_id(),
        }
        head_object_mock.return_value = {"ETag": any_etag()}
        expected_payload_log = dumps({"payload": body})

        with patch.object(self.logger, "debug") as logger_mock, patch(
            "backend.import_dataset.task.validate"
        ), patch("backend.import_dataset.task.smart_open"):

            # When
            lambda_handler(body, any_lambda_context())

            # Then
            logger_mock.assert_any_call(expected_payload_log)

    @patch("backend.import_dataset.task.validate")
    def should_log_schema_validation_warning(self, validate_schema_mock: MagicMock) -> None:
        # Given

        error_message = "Some error message"
        validate_schema_mock.side_effect = ValidationError(error_message)
        expected_log = dumps({"error": error_message})

        with patch.object(self.logger, "warning") as logger_mock:
            # When
            lambda_handler(
                {"metadata_url": any_s3_url(), "version_id": any_dataset_version_id()},
                any_lambda_context(),
            )

            # Then
            logger_mock.assert_any_call(expected_log)

    @patch("backend.import_dataset.task.S3CONTROL_CLIENT.create_job")
    @patch("backend.import_dataset.task.S3_CLIENT.head_object")
    @mark.infrastructure
    def should_log_assets_added_to_manifest(
        self,
        head_object_mock: MagicMock,
        create_job_mock: MagicMock,  # pylint:disable=unused-argument
        subtests: SubTests,
    ) -> None:
        # Given
        dataset_id = any_dataset_id()
        version_id = any_dataset_version_id()
        asset_id = f"DATASET#{dataset_id}#VERSION#{version_id}"
        head_object_mock.return_value = {"ETag": any_etag()}

        with ProcessingAsset(
            asset_id=asset_id, multihash=None, url=any_s3_url()
        ) as metadata_processing_asset, ProcessingAsset(
            asset_id=asset_id,
            multihash=any_hex_multihash(),
            url=any_s3_url(),
        ) as processing_asset:

            expected_asset_log = dumps({"Adding file to manifest": processing_asset.url})
            expected_metadata_log = dumps(
                {"Adding file to manifest": metadata_processing_asset.url}
            )

            with patch.object(self.logger, "debug") as logger_mock, patch(
                "backend.import_dataset.task.smart_open"
            ):
                # When
                lambda_handler(
                    {
                        "dataset_id": dataset_id,
                        "metadata_url": any_s3_url(),
                        "version_id": version_id,
                    },
                    any_lambda_context(),
                )

                # Then
                with subtests.test():
                    logger_mock.assert_any_call(expected_asset_log)
                with subtests.test():
                    logger_mock.assert_any_call(expected_metadata_log)

    @patch("backend.import_dataset.task.S3CONTROL_CLIENT.create_job")
    @patch("backend.import_dataset.task.S3_CLIENT.head_object")
    @mark.infrastructure
    def should_log_s3_batch_response(
        self, head_object_mock: MagicMock, create_job_mock: MagicMock
    ) -> None:
        # Given

        create_job_mock.return_value = response = {"JobId": "Some Response"}
        expected_response_log = json.dumps({"s3 batch response": response})
        head_object_mock.return_value = {"ETag": any_etag()}

        with patch.object(self.logger, "debug") as logger_mock, patch(
            "backend.import_dataset.task.smart_open"
        ):

            # When
            lambda_handler(
                {
                    "dataset_id": any_dataset_id(),
                    "metadata_url": any_s3_url(),
                    "version_id": any_dataset_version_id(),
                },
                any_lambda_context(),
            )

            # Then
            logger_mock.assert_any_call(expected_response_log)
