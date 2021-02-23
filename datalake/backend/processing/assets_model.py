from os import environ

from pynamodb.attributes import UnicodeAttribute
from pynamodb.models import Model


class ProcessingAssetsModel(Model):
    class Meta:  # pylint:disable=too-few-public-methods
        environment_name = environ["DEPLOY_ENV"]
        table_name = f"{environment_name}-processing-assets"
        region = "ap-southeast-2"  # TODO: don't hardcode region

    pk = UnicodeAttribute(hash_key=True)
    sk = UnicodeAttribute(range_key=True)
    url = UnicodeAttribute()
    multihash = UnicodeAttribute()
