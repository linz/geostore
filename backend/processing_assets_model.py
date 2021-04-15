"""Dataset object DynamoDB model."""
from enum import Enum
from os import environ
from typing import Type

from pynamodb.attributes import UnicodeAttribute
from pynamodb.models import Model


class ProcessingAssetType(Enum):
    DATA = "DATA_ITEM_INDEX"
    METADATA = "METADATA_ITEM_INDEX"


class ProcessingAssetsModelBase(Model):
    pk = UnicodeAttribute(hash_key=True)
    sk = UnicodeAttribute(range_key=True)
    url = UnicodeAttribute()
    multihash = UnicodeAttribute(null=True)


def processing_assets_model_with_meta(assets_table_name: str) -> Type[ProcessingAssetsModelBase]:
    class ProcessingAssetsModel(ProcessingAssetsModelBase):
        class Meta:  # pylint:disable=too-few-public-methods
            table_name = assets_table_name
            region = environ["AWS_DEFAULT_REGION"]

    return ProcessingAssetsModel
