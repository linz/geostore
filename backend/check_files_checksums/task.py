#!/usr/bin/env python3
import sys
from argparse import ArgumentParser, Namespace

from ..log import set_up_logging
from ..processing_assets_model import ProcessingAssetType
from ..validation_results_model import ValidationResultFactory
from .utils import ChecksumValidator, get_job_offset

LOGGER = set_up_logging(__name__)


def parse_arguments() -> Namespace:
    argument_parser = ArgumentParser()
    argument_parser.add_argument("--dataset-id", required=True)
    argument_parser.add_argument("--version-id", required=True)
    argument_parser.add_argument("--first-item", type=int, required=True)
    argument_parser.add_argument("--results-table-name", required=True)
    argument_parser.add_argument("--assets-table-name", required=True)
    return argument_parser.parse_args()


def main() -> int:
    arguments = parse_arguments()

    index = arguments.first_item + get_job_offset()
    hash_key = f"DATASET#{arguments.dataset_id}#VERSION#{arguments.version_id}"
    range_key = f"{ProcessingAssetType.DATA.value}#{index}"

    validation_result_factory = ValidationResultFactory(hash_key, arguments.results_table_name)

    checksum_validator = ChecksumValidator(
        arguments.assets_table_name, validation_result_factory, LOGGER
    )

    checksum_validator.validate(hash_key, range_key)

    return 0


if __name__ == "__main__":
    sys.exit(main())
