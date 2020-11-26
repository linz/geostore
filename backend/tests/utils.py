from random import choice

from ..endpoints.datasets.common import DATASET_TYPES


def any_valid_dataset_type() -> str:
    return choice(DATASET_TYPES)
