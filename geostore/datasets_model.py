from os import environ
from typing import Any, Dict, Optional, Tuple, Type

from pynamodb.attributes import UTCDateTimeAttribute, UnicodeAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex
from pynamodb.models import MetaModel, Model
from ulid import ULID, new

from .aws_keys import AWS_DEFAULT_REGION_KEY
from .clock import now
from .dataset_properties import DATASET_KEY_SEPARATOR
from .models import DATASET_ID_PREFIX, DB_KEY_SEPARATOR
from .parameter_store import ParameterName, get_param


def human_readable_ulid(ulid: ULID) -> str:
    """
    Formats the timestamp part of the ULID as a human readable datetime. Uses "T" as the date/time
    separator as per RFC3339, hyphen as the datetime field separator to ensure broad filesystem
    compatibility, and underscore as the datetime/randomness separator.

    ULIDs have millisecond timestamps, but strftime can only format microseconds, so we need to chop
    off the last three characters.
    """
    datetime_string = ulid.timestamp().datetime.strftime("%Y-%m-%dT%H-%M-%S-%f")[:-3]
    return f"{datetime_string}Z_{ulid.randomness()}"


# TODO: Remove inherit-non-class when astroid is at version 2.6 or later pylint:disable=fixme
class DatasetsTitleIdx(
    GlobalSecondaryIndex["DatasetsModelBase"]
):  # pylint:disable=too-few-public-methods,inherit-non-class
    """Dataset title global index."""

    class Meta:  # pylint:disable=too-few-public-methods
        """Meta class."""

        index_name = "datasets_title"
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    title = UnicodeAttribute(hash_key=True)


class DatasetsModelBase(Model):
    """Dataset model."""

    id = UnicodeAttribute(
        hash_key=True,
        attr_name="pk",
        default_for_new=lambda: f"{DATASET_ID_PREFIX}{new()}",
    )
    title = UnicodeAttribute()
    created_at = UTCDateTimeAttribute(default_for_new=now)
    updated_at = UTCDateTimeAttribute(default=now)

    datasets_title_idx: DatasetsTitleIdx

    def as_dict(self) -> Dict[str, Any]:
        serialized = self.serialize()
        result: Dict[str, Any] = {key: value["S"] for key, value in serialized.items()}
        result["id"] = self.dataset_id
        return result

    @property
    def dataset_id(self) -> str:
        """Dataset ID value."""
        return str(self.id).split(DB_KEY_SEPARATOR)[1]

    @property
    def dataset_prefix(self) -> str:
        """Dataset prefix value."""
        return f"{self.title}{DATASET_KEY_SEPARATOR}{self.dataset_id}"


class DatasetsModelMeta(MetaModel):
    def __new__(
        cls,
        name: str,
        bases: Tuple[Type[object], ...],
        namespace: Dict[str, Any],
        discriminator: Optional[Any] = None,
    ) -> "DatasetsModelMeta":
        namespace["Meta"] = type(
            "Meta",
            (),
            {
                "table_name": get_param(ParameterName.STORAGE_DATASETS_TABLE_NAME),
                "region": environ[AWS_DEFAULT_REGION_KEY],
            },
        )
        klass: "DatasetsModelMeta" = MetaModel.__new__(  # type: ignore[no-untyped-call]
            cls, name, bases, namespace, discriminator=discriminator
        )
        return klass


def datasets_model_with_meta() -> Type[DatasetsModelBase]:
    class DatasetModel(DatasetsModelBase, metaclass=DatasetsModelMeta):
        datasets_title_idx = DatasetsTitleIdx()

    return DatasetModel
