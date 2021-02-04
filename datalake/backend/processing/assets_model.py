from pynamodb.attributes import UnicodeAttribute
from pynamodb.models import Model

PROCESSING_ASSETS_TABLE_NAME = "processing_assets"


class ProcessingAssetsModel(Model):
    class Meta:  # pylint:disable=too-few-public-methods
        table_name = PROCESSING_ASSETS_TABLE_NAME
        region = "ap-southeast-2"  # TODO: don't hardcode region

    pk = UnicodeAttribute(hash_key=True)
    sk = UnicodeAttribute(range_key=True)
    url = UnicodeAttribute()
    multihash = UnicodeAttribute()
