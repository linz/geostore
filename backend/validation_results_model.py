from pynamodb.attributes import BooleanAttribute, UnicodeAttribute
from pynamodb.models import Model

from .resources import ResourceName


class ValidationResultsModel(Model):
    class Meta:  # pylint:disable=too-few-public-methods
        table_name = ResourceName.VALIDATION_RESULTS_TABLE_NAME.value
        region = "ap-southeast-2"  # TODO: don't hardcode region

    pk = UnicodeAttribute(hash_key=True)
    sk = UnicodeAttribute(range_key=True)
    success = BooleanAttribute()
