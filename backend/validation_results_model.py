from pynamodb.attributes import UnicodeAttribute
from pynamodb.indexes import AllProjection, GlobalSecondaryIndex
from pynamodb.models import Model

from .resources import ResourceName


# TODO: Remove inherit-non-class when https://github.com/PyCQA/pylint/issues/3950 is fixed
class ValidationOutcomeIdx(
    GlobalSecondaryIndex["ValidationResultsModel"]
):  # pylint:disable=too-few-public-methods,inherit-non-class
    class Meta:  # pylint:disable=too-few-public-methods

        index_name = "validation_outcome"
        read_capacity_units = 1
        write_capacity_units = 1
        projection = AllProjection()

    pk = UnicodeAttribute(hash_key=True)
    status = UnicodeAttribute(range_key=True)


class ValidationResultsModel(Model):
    class Meta:  # pylint:disable=too-few-public-methods
        table_name = ResourceName.VALIDATION_RESULTS_TABLE_NAME.value
        region = "ap-southeast-2"  # TODO: don't hardcode region

    pk = UnicodeAttribute(hash_key=True)
    sk = UnicodeAttribute(range_key=True)
    status = UnicodeAttribute()
    info = UnicodeAttribute(null=True)

    validation_outcome_index = ValidationOutcomeIdx()
