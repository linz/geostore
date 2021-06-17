#!/usr/bin/env python3
from argparse import ArgumentParser, Namespace

from ..log import set_up_logging
from ..models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR, VERSION_ID_PREFIX
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


def main() -> None:
    arguments = parse_arguments()

    index = arguments.first_item + get_job_offset()
    hash_key = (
        f"{DATASET_ID_PREFIX}{arguments.dataset_id}"
        f"{DB_KEY_SEPARATOR}{VERSION_ID_PREFIX}{arguments.version_id}"
    )
    range_key = f"{ProcessingAssetType.DATA.value}{DB_KEY_SEPARATOR}{index}"

    validation_result_factory = ValidationResultFactory(hash_key, arguments.results_table_name)

    checksum_validator = ChecksumValidator(
        arguments.assets_table_name, validation_result_factory, LOGGER
    )

    checksum_validator.validate(hash_key, range_key)


if __name__ == "__main__":
    main()
