import string
from random import choice

from ..endpoints.datasets.common import DATASET_TYPES


def random_string(length: int) -> str:
    return "".join(choice(string.printable) for _ in range(length))


def any_valid_dataset_type() -> str:
    return choice(DATASET_TYPES)


def any_dataset_title() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_dataset_owning_group() -> str:
    """Arbitrary-length string"""
    return random_string(20)
