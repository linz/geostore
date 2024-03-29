"""Dataset object DynamoDB model."""
from dataclasses import dataclass
from enum import Enum
from os import environ
from typing import Optional, Type

from pynamodb.attributes import BooleanAttribute, UnicodeAttribute
from pynamodb.models import Model

from .aws_keys import AWS_DEFAULT_REGION_KEY
from .parameter_store import ParameterName, get_param


class ProcessingAssetType(Enum):
    DATA = "DATA_ITEM_INDEX"
    METADATA = "METADATA_ITEM_INDEX"


class ProcessingAssetsModelBase(Model):
    pk = UnicodeAttribute(hash_key=True)
    sk = UnicodeAttribute(range_key=True)
    url = UnicodeAttribute()
    filename = UnicodeAttribute()
    multihash = UnicodeAttribute(null=True)
    exists_in_staging = BooleanAttribute(null=True)
    replaced_in_new_version = BooleanAttribute(null=True)


def processing_assets_model_with_meta(
    *, assets_table_name: Optional[str] = None
) -> Type[ProcessingAssetsModelBase]:
    if assets_table_name is None:
        assets_table_name = get_param(ParameterName.PROCESSING_ASSETS_TABLE_NAME)

    class ProcessingAssetsModel(ProcessingAssetsModelBase):
        @dataclass
        class Meta:
            table_name = assets_table_name
            region = environ[AWS_DEFAULT_REGION_KEY]

    return ProcessingAssetsModel
