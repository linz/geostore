from pynamodb.attributes import BooleanAttribute, UnicodeAttribute
from pynamodb.models import Model

from .parameter_store import ParameterName, get_param


class ValidationResultsModel(Model):
    class Meta:  # pylint:disable=too-few-public-methods
        table_name = get_param(ParameterName.VALIDATION_RESULTS_TABLE_NAME.value)
        region = "ap-southeast-2"  # TODO: don't hardcode region

    pk = UnicodeAttribute(hash_key=True)
    sk = UnicodeAttribute(range_key=True)
    success = BooleanAttribute()
