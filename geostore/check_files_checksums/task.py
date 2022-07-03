#!/usr/bin/env python3
from logging import Logger
from optparse import OptionParser, Values  # pylint: disable=deprecated-module

from linz_logger import get_log

from ..models import DB_KEY_SEPARATOR
from ..processing_assets_model import ProcessingAssetType
from ..s3_utils import get_s3_url_reader
from ..step_function import AssetGarbageCollector, get_hash_key
from ..validation_results_model import ValidationResultFactory
from .utils import ChecksumUtils, get_job_offset

ASSETS_TABLE_NAME_ARGUMENT = "--assets-table-name"
CURRENT_VERSION_ID_ARGUMENT = "--current-version-id"
DATASET_ID_ARGUMENT = "--dataset-id"
DATASET_TITLE_ARGUMENT = "--dataset-title"
FIRST_ITEM_ARGUMENT = "--first-item"
NEW_VERSION_ID_ARGUMENT = "--new-version-id"
RESULTS_TABLE_NAME_ARGUMENT = "--results-table-name"
S3_ROLE_ARN_ARGUMENT = "--s3-role-arn"

LOGGER: Logger = get_log()


def parse_arguments() -> Values:
    parser = OptionParser()
    parser.add_option(DATASET_ID_ARGUMENT)
    parser.add_option(NEW_VERSION_ID_ARGUMENT)
    parser.add_option(CURRENT_VERSION_ID_ARGUMENT)
    parser.add_option(DATASET_TITLE_ARGUMENT)
    parser.add_option(FIRST_ITEM_ARGUMENT, type=int)
    parser.add_option(RESULTS_TABLE_NAME_ARGUMENT)
    parser.add_option(ASSETS_TABLE_NAME_ARGUMENT)
    parser.add_option(S3_ROLE_ARN_ARGUMENT)
    (options, _args) = parser.parse_args()

    for option in parser.option_list:
        if option.dest is not None:
            assert hasattr(options, option.dest)

    return options


def main() -> None:
    arguments = parse_arguments()

    index = arguments.first_item + get_job_offset()
    hash_key = get_hash_key(arguments.dataset_id, arguments.new_version_id)
    range_key = f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}{index}"
    validation_result_factory = ValidationResultFactory(hash_key, arguments.results_table_name)
    s3_url_reader = get_s3_url_reader(arguments.s3_role_arn, arguments.dataset_title, LOGGER)

    asset_garbage_collector = AssetGarbageCollector(
        arguments.dataset_id,
        arguments.current_version_id,
        ProcessingAssetType.DATA,
        LOGGER,
        arguments.assets_table_name,
    )

    utils = ChecksumUtils(
        arguments.assets_table_name,
        validation_result_factory,
        s3_url_reader,
        asset_garbage_collector,
        LOGGER,
    )
    utils.run(hash_key, range_key)


if __name__ == "__main__":
    main()
