from typing import Any, Dict

from .model import DatasetModel


def serialize_dataset(dataset: DatasetModel) -> Dict[str, Any]:
    resp_item = dict(dataset)
    resp_item["id"] = dataset.dataset_id
    resp_item["type"] = dataset.dataset_type
    return resp_item
