from os.path import basename
from unittest.mock import MagicMock, patch

from pytest import mark
from pytest_subtests import SubTests

from geostore.models import DB_KEY_SEPARATOR
from geostore.processing_assets_model import ProcessingAssetType, processing_assets_model_with_meta
from geostore.step_function import AssetGarbageCollector, get_hash_key
from geostore.step_function_keys import CURRENT_VERSION_EMPTY_VALUE
from tests.aws_utils import ProcessingAsset, any_s3_url
from tests.stac_generators import any_dataset_id, any_dataset_version_id


@mark.infrastructure
def should_mark_asset_as_replaced(subtests: SubTests) -> None:
    # Given

    dataset_id = any_dataset_id()
    current_version_id = any_dataset_version_id()
    url = any_s3_url()
    filename = basename(url)
    logger_mock = MagicMock()

    expected_log_message = (
        f"Dataset: '{dataset_id}' "
        f"Version: '{current_version_id}' "
        f"Filename: '{filename}' has been marked as replaced"
    )

    hash_key = get_hash_key(dataset_id, current_version_id)
    processing_assets_model = processing_assets_model_with_meta()
    expected_metadata_item = processing_assets_model(
        hash_key=hash_key,
        range_key=f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}0",
        url=url,
        filename=filename,
        replaced_in_new_version=True,
    )

    with ProcessingAsset(
        asset_id=hash_key,
        url=url,
    ):

        # When
        AssetGarbageCollector(
            dataset_id, current_version_id, ProcessingAssetType.METADATA, logger_mock
        ).mark_asset_as_replaced(filename)

        # Then
        with subtests.test(msg="Log is recorded"):
            logger_mock.debug.assert_called_once_with(expected_log_message)

        actual_first_version_metadata_item = processing_assets_model.query(
            hash_key,
            processing_assets_model.sk.startswith(
                f"{ProcessingAssetType.METADATA.value}{DB_KEY_SEPARATOR}"
            ),
            consistent_read=True,
        ).next()

        with subtests.test(msg=f"Metadata {actual_first_version_metadata_item.pk}"):
            assert (
                actual_first_version_metadata_item.attribute_values
                == expected_metadata_item.attribute_values
            )


@mark.infrastructure
def should_do_nothing_if_no_asset_returned(subtests: SubTests) -> None:
    # Given

    dataset_id = any_dataset_id()
    current_version_id = any_dataset_version_id()
    url = any_s3_url()
    filename = basename(url)
    logger_mock = MagicMock()

    # When
    AssetGarbageCollector(
        dataset_id, current_version_id, ProcessingAssetType.METADATA, logger_mock
    ).mark_asset_as_replaced(filename)

    # Then
    with subtests.test(msg="Log is recorded"):
        logger_mock.debug.assert_not_called()


@patch("geostore.step_function.processing_assets_model_with_meta")
def should_return_early_if_no_dataset_version(
    processing_assets_model_mock: MagicMock, subtests: SubTests
) -> None:
    # Given
    dataset_id = any_dataset_id()
    current_version_id = CURRENT_VERSION_EMPTY_VALUE
    url = any_s3_url()
    filename = basename(url)
    logger_mock = MagicMock()

    # When
    AssetGarbageCollector(
        dataset_id, current_version_id, ProcessingAssetType.METADATA, logger_mock
    ).mark_asset_as_replaced(filename)

    # Then
    with subtests.test(msg="db record is not queried"):
        processing_assets_model_mock.return_value.assert_not_called()

    with subtests.test(msg="Log is not recorded"):
        logger_mock.debug.assert_not_called()
