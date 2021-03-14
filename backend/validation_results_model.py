from enum import Enum
from typing import Optional

from pynamodb.attributes import MapAttribute, UnicodeAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex
from pynamodb.models import Model

from .check import Check
from .resources import ResourceName
from .types import JsonObject


class ValidationResult(Enum):
    FAILED = "Failed"
    PASSED = "Passed"


# TODO: Remove inherit-non-class when https://github.com/PyCQA/pylint/issues/3950 is fixed
class ValidationOutcomeIdx(
    GlobalSecondaryIndex["ValidationResultsModel"]
):  # pylint:disable=too-few-public-methods,inherit-non-class
    class Meta:  # pylint:disable=too-few-public-methods

        index_name = "validation_outcome"
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    pk = UnicodeAttribute(hash_key=True, attr_name="pk")
    result = UnicodeAttribute(range_key=True, attr_name="result")


class ValidationResultsModel(Model):
    class Meta:  # pylint:disable=too-few-public-methods
        table_name = ResourceName.VALIDATION_RESULTS_TABLE_NAME.value
        region = "ap-southeast-2"  # TODO: don't hardcode region

    pk = UnicodeAttribute(hash_key=True)
    sk = UnicodeAttribute(range_key=True)
    result = UnicodeAttribute()
    # TODO: Remove type-arg when https://github.com/pynamodb/PynamoDB/issues/682 is fixed
    details: MapAttribute = MapAttribute(null=True)  # type: ignore[type-arg]

    validation_outcome_index = ValidationOutcomeIdx()


class ValidationResultFactory:  # pylint:disable=too-few-public-methods
    def __init__(self, hash_key: str):
        self.hash_key = hash_key

    def save(
        self, url: str, check: Check, result: ValidationResult, details: Optional[JsonObject] = None
    ) -> None:
        ValidationResultsModel(
            pk=self.hash_key,
            sk=f"CHECK#{check.value}#URL#{url}",
            result=result.value,
            details=details,
        ).save()
