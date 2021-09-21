from backend.step_function import get_hash_key

from .stac_generators import any_dataset_id, any_dataset_version_id


def any_hash_key() -> str:
    return get_hash_key(any_dataset_id(), any_dataset_version_id())
