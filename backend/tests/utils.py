import string
from datetime import datetime, timedelta, timezone
from random import choice, randrange
from types import TracebackType
from typing import Optional, Type
from uuid import uuid4

from ..endpoints.datasets.common import DATASET_TYPES
from ..endpoints.datasets.model import DatasetModel

REFERENCE_DATETIME = datetime(2000, 1, 1, tzinfo=timezone.utc)


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


def any_past_datetime() -> datetime:
    return REFERENCE_DATETIME - timedelta(seconds=randrange(60_000_000_000))  # Back to year 98


def any_dataset_owning_group() -> str:
    """Arbitrary-length string"""
    return random_string(20)


class Dataset:
    def __init__(
        self,
        dataset_id: Optional[str] = None,
        dataset_type: Optional[str] = None,
        title: Optional[str] = None,
        owning_group: Optional[str] = None,
    ):
        if dataset_id is None:
            dataset_id = any_dataset_id()

        if dataset_type is None:
            dataset_type = any_valid_dataset_type()

        if title is None:
            title = any_dataset_title()

        if owning_group is None:
            owning_group = any_dataset_owning_group()

        self.model = DatasetModel(
            id=f"DATASET#{dataset_id}",
            type=f"TYPE#{dataset_type}",
            title=title,
            owning_group=owning_group,
            created_at=any_past_datetime(),
            updated_at=any_past_datetime(),
        )

    def __enter__(self) -> DatasetModel:
        self.model.save()
        return self.model

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        self.model.delete()

        return False  # Propagate exception
