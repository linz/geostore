"""List all datasets function."""

from ..model import DatasetModel
from ..utils import JSON_OBJECT, success_response


def list_datasets() -> JSON_OBJECT:
    """GET: List all Datasets."""

    # list all datasets
    datasets = DatasetModel.scan(
        filter_condition=DatasetModel.id.startswith("DATASET#")
        & DatasetModel.type.startswith("TYPE#")
    )

    # return response
    resp_body = []
    for dataset in datasets:
        resp_item = dataset.serialize()
        resp_body.append(resp_item)

    return success_response(200, resp_body)
