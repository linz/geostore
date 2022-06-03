import sys
from datetime import timedelta
from hashlib import sha256
from io import BytesIO
from os import environ
from typing import TYPE_CHECKING, Any, List, Optional, Sequence, Text, Type
from unittest.mock import MagicMock, call, patch

from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from multihash import SHA2_256
from pynamodb.expressions.condition import Condition
from pynamodb.expressions.update import Action
from pynamodb.models import _T, _KeyType
from pynamodb.settings import OperationSettings
from pytest import mark, raises
from pytest_subtests import SubTests

from geostore.api_keys import MESSAGE_KEY
from geostore.check import Check
from geostore.check_files_checksums.task import main
from geostore.check_files_checksums.utils import (
    ARRAY_INDEX_VARIABLE_NAME,
    ChecksumMismatchError,
    ChecksumValidator,
    get_job_offset,
)
from geostore.logging_keys import LOG_MESSAGE_VALIDATION_COMPLETE
from geostore.models import CHECK_ID_PREFIX, DB_KEY_SEPARATOR, URL_ID_PREFIX
from geostore.parameter_store import ParameterName, get_param
from geostore.processing_assets_model import (
    ProcessingAssetType,
    ProcessingAssetsModelBase,
    processing_assets_model_with_meta,
)
from geostore.resources import Resource
from geostore.s3 import CHUNK_SIZE, S3_URL_PREFIX
from geostore.step_function import Outcome, get_hash_key
from geostore.validation_results_model import ValidationResult, validation_results_model_with_meta

from .aws_utils import (
    Dataset,
    MockGeostoreS3Response,
    MockJSONURLReader,
    MockValidationResultFactory,
    ProcessingAsset,
    S3Object,
    any_batch_job_array_index,
    any_role_arn,
    any_s3_url,
    any_table_name,
    get_s3_role_arn,
)
from .general_generators import any_boolean, any_file_contents, any_program_name, any_safe_filename
from .stac_generators import (
    any_dataset_id,
    any_dataset_prefix,
    any_dataset_version_id,
    any_hex_multihash,
    any_sha256_hex_digest,
    sha256_hex_digest_to_multihash,
)

SHA256_CHECKSUM_BYTE_COUNT = 32

if TYPE_CHECKING:
    from botocore.exceptions import ClientErrorResponseError, ClientErrorResponseTypeDef
else:
    ClientErrorResponseError = ClientErrorResponseTypeDef = dict


def should_return_offset_from_array_index_variable() -> None:
    index = any_batch_job_array_index()
    with patch.dict(environ, {ARRAY_INDEX_VARIABLE_NAME: str(index)}):
        assert get_job_offset() == index


def should_return_default_offset_to_zero() -> None:
    environ.pop(ARRAY_INDEX_VARIABLE_NAME, default="")

    assert get_job_offset() == 0


@patch("geostore.check_files_checksums.utils.ChecksumValidator.validate_url_multihash")
@patch("geostore.check_files_checksums.utils.processing_assets_model_with_meta")
@patch("geostore.check_files_checksums.task.ValidationResultFactory")
@patch("pynamodb.connection.base.get_session", MagicMock())
@patch("pynamodb.connection.table.Connection", MagicMock())
def should_validate_given_index(
    validation_results_factory_mock: MagicMock,
    processing_assets_model_mock: MagicMock,
    validate_url_multihash_mock: MagicMock,
    subtests: SubTests,
) -> None:
    # Given
    dataset_id = any_dataset_id()
    version_id = any_dataset_version_id()
    hash_key_ = get_hash_key(dataset_id, version_id)

    url = any_s3_url()
    hex_multihash = any_hex_multihash()

    array_index = "1"

    def processing_assets_model_with_meta_mock(
        given_hash_key: str, given_array_index: str
    ) -> Type[ProcessingAssetsModelBase]:
        class ProcessingAssetsModelMock(ProcessingAssetsModelBase):
            class Meta:  # pylint:disable=too-few-public-methods
                table_name = any_table_name()

            @classmethod
            def get(  # pylint:disable=too-many-arguments
                cls: Type[_T],
                hash_key: _KeyType,
                range_key: Optional[_KeyType] = None,
                consistent_read: bool = False,
                attributes_to_get: Optional[Sequence[Text]] = None,
                settings: OperationSettings = OperationSettings.default,
            ) -> _T:
                assert hash_key == given_hash_key
                assert (
                    range_key
                    == f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}{given_array_index}"
                )
                return cls(
                    hash_key=given_hash_key,
                    range_key=f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}1",
                    url=url,
                    multihash=hex_multihash,
                    exists_in_staging=any_boolean(),
                )

            def update(
                self,
                actions: List[Action],
                condition: Optional[Condition] = None,
                settings: OperationSettings = OperationSettings.default,
            ) -> Any:
                return self

        return ProcessingAssetsModelMock

    processing_assets_model_mock.return_value = processing_assets_model_with_meta_mock(
        hash_key_, array_index
    )
    validate_url_multihash_mock.return_value = True
    validation_results_table_name = any_table_name()
    expected_calls = [
        call(hash_key_, validation_results_table_name),
        call().save(url, Check.CHECKSUM, ValidationResult.PASSED),
    ]

    # When
    sys.argv = [
        any_program_name(),
        f"--dataset-id={dataset_id}",
        f"--version-id={version_id}",
        f"--dataset-prefix={any_dataset_prefix()}",
        "--first-item=0",
        f"--assets-table-name={any_table_name()}",
        f"--results-table-name={validation_results_table_name}",
        f"--s3-role-arn={any_role_arn()}",
    ]
    with patch("geostore.check_files_checksums.task.LOGGER.info") as info_log_mock, patch.dict(
        environ, {ARRAY_INDEX_VARIABLE_NAME: array_index}
    ), patch("geostore.check_files_checksums.task.get_s3_url_reader"):
        # Then
        main()

        with subtests.test(msg="Log message"):
            info_log_mock.assert_any_call(
                LOG_MESSAGE_VALIDATION_COMPLETE, extra={"outcome": Outcome.PASSED}
            )

    with subtests.test(msg="Validate checksums"):
        assert validate_url_multihash_mock.mock_calls == [call(url, hex_multihash)]

    with subtests.test(msg="Validation result"):
        assert validation_results_factory_mock.mock_calls == expected_calls


@patch("geostore.check_files_checksums.utils.ChecksumValidator.validate_url_multihash")
@patch("geostore.check_files_checksums.utils.processing_assets_model_with_meta")
@patch("geostore.check_files_checksums.task.ValidationResultFactory")
def should_log_error_when_validation_fails(
    validation_results_factory_mock: MagicMock,
    processing_assets_model_mock: MagicMock,
    validate_url_multihash_mock: MagicMock,
    subtests: SubTests,
) -> None:
    # Given
    actual_hex_digest = any_sha256_hex_digest()
    expected_hex_digest = any_sha256_hex_digest()
    expected_hex_multihash = sha256_hex_digest_to_multihash(expected_hex_digest)
    dataset_id = any_dataset_id()
    dataset_version_id = any_dataset_version_id()
    hash_key = get_hash_key(dataset_id, dataset_version_id)
    url = any_s3_url()
    processing_assets_model_mock.return_value.get.return_value = ProcessingAssetsModelBase(
        hash_key=hash_key,
        range_key=f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}0",
        url=url,
        multihash=expected_hex_multihash,
    )
    expected_details = {
        MESSAGE_KEY: f"Checksum mismatch: expected {expected_hex_digest}, got {actual_hex_digest}"
    }
    validate_url_multihash_mock.side_effect = ChecksumMismatchError(actual_hex_digest)
    # When

    validation_results_table_name = any_table_name()
    sys.argv = [
        any_program_name(),
        f"--dataset-id={dataset_id}",
        f"--version-id={dataset_version_id}",
        f"--dataset-prefix={any_dataset_prefix()}",
        "--first-item=0",
        f"--assets-table-name={any_table_name()}",
        f"--results-table-name={validation_results_table_name}",
        f"--s3-role-arn={any_role_arn()}",
    ]

    # Then
    with patch("geostore.check_files_checksums.task.LOGGER.error") as error_log_mock, patch.dict(
        environ, {ARRAY_INDEX_VARIABLE_NAME: "0"}
    ), patch("geostore.check_files_checksums.task.get_s3_url_reader"):
        main()

        with subtests.test(msg="Log message"):
            error_log_mock.assert_any_call(
                LOG_MESSAGE_VALIDATION_COMPLETE,
                extra={"outcome": Outcome.FAILED, "error": expected_details},
            )

    with subtests.test(msg="Validation result"):
        assert validation_results_factory_mock.mock_calls == [
            call(hash_key, validation_results_table_name),
            call().save(url, Check.CHECKSUM, ValidationResult.FAILED, details=expected_details),
        ]


@mark.timeout(timedelta(minutes=20).total_seconds())
@mark.infrastructure
def should_successfully_validate_asset_not_in_staging(
    subtests: SubTests,
) -> None:
    # Given
    dataset_version_id = any_dataset_version_id()
    storage_asset_filename = any_safe_filename()
    storage_asset_content = any_file_contents()
    storage_asset_multihash = sha256_hex_digest_to_multihash(
        sha256(storage_asset_content).hexdigest()
    )

    asset_staging_url = (
        f"{S3_URL_PREFIX}{Resource.STAGING_BUCKET_NAME.resource_name}/"
        f"{any_safe_filename()}/{storage_asset_filename}"
    )

    with Dataset() as dataset, S3Object(
        BytesIO(initial_bytes=storage_asset_content),
        Resource.STORAGE_BUCKET_NAME.resource_name,
        f"{dataset.dataset_prefix}/{storage_asset_filename}",
    ):

        hash_key = get_hash_key(dataset.dataset_id, dataset_version_id)
        assets_table_name = get_param(ParameterName.PROCESSING_ASSETS_TABLE_NAME)
        results_table_name = get_param(ParameterName.STORAGE_VALIDATION_RESULTS_TABLE_NAME)

        with ProcessingAsset(
            asset_id=hash_key, url=asset_staging_url, multihash=storage_asset_multihash
        ):

            # When
            sys.argv = [
                any_program_name(),
                f"--dataset-id={dataset.dataset_id}",
                f"--version-id={dataset_version_id}",
                f"--dataset-prefix={dataset.dataset_prefix}",
                "--first-item=0",
                f"--assets-table-name={assets_table_name}",
                f"--results-table-name={results_table_name}",
                f"--s3-role-arn={get_s3_role_arn()}",
            ]

            main()

            # Then
            with subtests.test(msg="Storage asset validation results"):
                validation_results_model = validation_results_model_with_meta()

                assert (
                    validation_results_model.get(
                        hash_key=hash_key,
                        range_key=(
                            f"{CHECK_ID_PREFIX}{Check.CHECKSUM.value}"
                            f"{DB_KEY_SEPARATOR}{URL_ID_PREFIX}{asset_staging_url}"
                        ),
                        consistent_read=True,
                    ).result
                    == ValidationResult.PASSED.value
                )

            with subtests.test(msg="Item has been deleted from Assets table"):
                processing_assets_model = processing_assets_model_with_meta()

                assert (
                    processing_assets_model.get(
                        hash_key,
                        range_key=f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}0",
                    ).exists_in_staging
                    is False
                )


@mark.infrastructure
@patch("geostore.check_files_checksums.task.get_s3_url_reader")
@patch("geostore.check_files_checksums.utils.processing_assets_model_with_meta")
@patch("geostore.check_files_checksums.task.ValidationResultFactory")
def should_save_file_not_found_validation_results(
    validation_results_factory_mock: MagicMock,
    processing_assets_model_mock: MagicMock,
    get_s3_client_for_role_mock: MagicMock,
) -> None:

    dataset_version_id = any_dataset_version_id()
    storage_asset_filename = any_safe_filename()

    asset_staging_url = (
        f"{S3_URL_PREFIX}{Resource.STAGING_BUCKET_NAME.resource_name}/"
        f"{any_safe_filename()}/{storage_asset_filename}"
    )
    storage_asset_content = any_file_contents()

    storage_asset_multihash = sha256_hex_digest_to_multihash(
        sha256(storage_asset_content).hexdigest()
    )

    expected_error = ClientError(
        ClientErrorResponseTypeDef(
            Error=ClientErrorResponseError(Code="NoSuchKey", Message="TEST")
        ),
        operation_name="get_object",
    )

    get_s3_client_for_role_mock.return_value.side_effect = expected_error

    with Dataset() as dataset:
        hash_key = get_hash_key(dataset.dataset_id, dataset_version_id)

        processing_assets_model_mock.return_value.get.return_value = ProcessingAssetsModelBase(
            hash_key=hash_key,
            range_key=f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}0",
            url=asset_staging_url,
            multihash=storage_asset_multihash,
        )

        # When
        sys.argv = [
            any_program_name(),
            f"--dataset-id={dataset.dataset_id}",
            f"--version-id={dataset_version_id}",
            f"--dataset-prefix={dataset.dataset_prefix}",
            "--first-item=0",
            f"--assets-table-name={any_table_name()}",
            f"--results-table-name={any_table_name()}",
            f"--s3-role-arn={get_s3_role_arn()}",
        ]

        with raises(ClientError):
            main()

        validation_results_factory_mock.assert_has_calls(
            [
                call().save(
                    asset_staging_url,
                    Check.FILE_NOT_FOUND,
                    ValidationResult.FAILED,
                    details={
                        MESSAGE_KEY: f"Could not find asset file '{asset_staging_url}' "
                        f"in staging bucket or in the Geostore."
                    },
                ),
            ]
        )


def should_return_when_file_checksum_matches() -> None:
    file_contents = b"x" * (CHUNK_SIZE + 1)
    url = any_s3_url()
    s3_url_reader = MockJSONURLReader(
        {
            url: MockGeostoreS3Response(
                StreamingBody(BytesIO(initial_bytes=file_contents), len(file_contents)), True
            )
        }
    )

    multihash = (
        f"{SHA2_256:x}{SHA256_CHECKSUM_BYTE_COUNT:x}"
        "c6d8e9905300876046729949cc95c2385221270d389176f7234fe7ac00c4e430"
    )

    with patch("geostore.check_files_checksums.utils.processing_assets_model_with_meta"):
        ChecksumValidator(
            any_table_name(), MockValidationResultFactory(), s3_url_reader, MagicMock()
        ).validate_url_multihash(url, multihash)


def should_raise_exception_when_checksum_does_not_match() -> None:
    url = any_s3_url()
    s3_url_reader = MockJSONURLReader(
        {url: MockGeostoreS3Response(StreamingBody(BytesIO(), 0), True)}
    )

    checksum = "0" * 64
    with raises(ChecksumMismatchError), patch(
        "geostore.check_files_checksums.utils.processing_assets_model_with_meta"
    ):
        ChecksumValidator(
            any_table_name(), MockValidationResultFactory(), s3_url_reader, MagicMock()
        ).validate_url_multihash(url, f"{SHA2_256:x}{SHA256_CHECKSUM_BYTE_COUNT:x}{checksum}")
