from enum import Enum
from os import environ
from typing import Any, Dict, Optional, Tuple, Type

from pynamodb.attributes import MapAttribute, UnicodeAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex
from pynamodb.models import MetaModel, Model

from .aws_keys import AWS_DEFAULT_REGION_KEY
from .check import Check
from .models import CHECK_ID_PREFIX, DB_KEY_SEPARATOR, URL_ID_PREFIX
from .parameter_store import ParameterName, get_param
from .types import JsonObject


class ValidationResult(Enum):
    FAILED = "Failed"
    PASSED = "Passed"


# TODO: Remove inherit-non-class when astroid is at version 2.6 or later pylint:disable=fixme
class ValidationOutcomeIdx(
    GlobalSecondaryIndex["ValidationResultsModelBase"]
):  # pylint:disable=too-few-public-methods,inherit-non-class
    class Meta:  # pylint:disable=too-few-public-methods

        index_name = "validation_outcome"
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    pk = UnicodeAttribute(hash_key=True, attr_name="pk")
    result = UnicodeAttribute(range_key=True, attr_name="result")


class ValidationResultsModelBase(Model):
    pk = UnicodeAttribute(hash_key=True)
    sk = UnicodeAttribute(range_key=True)
    result = UnicodeAttribute()
    # TODO: Remove type-arg when PynamoDB issue #920 is fixed pylint:disable=fixme
    details: MapAttribute[str, Any] = MapAttribute(null=True)  # type: ignore[no-untyped-call]

    validation_outcome_index: ValidationOutcomeIdx


def validation_results_model_with_meta(
    results_table_name: Optional[str] = None,
) -> Type[ValidationResultsModelBase]:
    if results_table_name is None:
        results_table_name = get_param(ParameterName.STORAGE_VALIDATION_RESULTS_TABLE_NAME)

    class ValidationResultsModelMeta(MetaModel):
        def __new__(
            cls,
            name: str,
            bases: Tuple[Type[object], ...],
            namespace: Dict[str, Any],
            discriminator: Optional[Any] = None,
        ) -> "ValidationResultsModelMeta":
            namespace["Meta"] = type(
                "Meta",
                (),
                {
                    "table_name": results_table_name,
                    "region": environ[AWS_DEFAULT_REGION_KEY],
                },
            )
            klass: "ValidationResultsModelMeta"
            klass = MetaModel.__new__(  # type: ignore[no-untyped-call]
                cls, name, bases, namespace, discriminator=discriminator
            )
            return klass

    class ValidationResultsModel(ValidationResultsModelBase, metaclass=ValidationResultsModelMeta):
        validation_outcome_index = ValidationOutcomeIdx()

    return ValidationResultsModel


class ValidationResultFactory:  # pylint:disable=too-few-public-methods
    def __init__(
        self,
        hash_key: str,
        results_table_name: str,
    ):

        self.hash_key = hash_key
        self.validation_results_model = validation_results_model_with_meta(results_table_name)

    def save(
        self, url: str, check: Check, result: ValidationResult, details: Optional[JsonObject] = None
    ) -> None:
        self.validation_results_model(
            pk=self.hash_key,
            sk=f"{CHECK_ID_PREFIX}{check.value}{DB_KEY_SEPARATOR}{URL_ID_PREFIX}{url}",
            result=result.value,
            details=details,
        ).save()
