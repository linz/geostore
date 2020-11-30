import string
from random import choice
from uuid import uuid4

from ..endpoints.datasets.common import DATASET_TYPES


def random_string(length: int) -> str:
    """
    Includes ASCII printable characters and the first printable character from several Unicode
    blocks <https://en.wikipedia.org/wiki/List_of_Unicode_characters>.
    """
    characters = f"{string.printable}¡ĀƀḂəʰͰἀЀ–⁰₠℀⅐←∀⌀①─▀■☀🬀✁ㄅﬀ"
    return "".join(choice(characters) for _ in range(length))


def any_dataset_id() -> str:
    return str(uuid4())


def any_valid_dataset_type() -> str:
    return choice(DATASET_TYPES)


def any_dataset_title() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_dataset_owning_group() -> str:
    """Arbitrary-length string"""
    return random_string(20)
