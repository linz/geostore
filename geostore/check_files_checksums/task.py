#!/usr/bin/env python3
from argparse import ArgumentParser, Namespace

from linz_logger import get_log

from ..models import DB_KEY_SEPARATOR
from ..processing_assets_model import ProcessingAssetType
from ..step_function import get_hash_key
from ..validation_results_model import ValidationResultFactory
from .utils import ChecksumValidator, get_job_offset

LOGGER = get_log()


def parse_arguments() -> Namespace:
    argument_parser = ArgumentParser()
    argument_parser.add_argument("--dataset-id", required=True)
    argument_parser.add_argument("--version-id", required=True)
    argument_parser.add_argument("--first-item", type=int, required=True)
    argument_parser.add_argument("--results-table-name", required=True)
    argument_parser.add_argument("--assets-table-name", required=True)
    argument_parser.add_argument("--s3-role-arn", required=True)
    return argument_parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    index = arguments.first_item + get_job_offset()
    hash_key = get_hash_key(arguments.dataset_id, arguments.version_id)
    range_key = f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}{index}"

    validation_result_factory = ValidationResultFactory(hash_key, arguments.results_table_name)

    checksum_validator = ChecksumValidator(
        arguments.assets_table_name, validation_result_factory, arguments.s3_role_arn, LOGGER
    )

    checksum_validator.validate(hash_key, range_key)


if __name__ == "__main__":
    main()
