from pytest import mark

from backend.datasets_model import datasets_model_with_meta


@mark.infrastructure
def should_create_unique_id_per_dataset() -> None:
    model = datasets_model_with_meta()
    first = model()
    second = model()

    assert first.dataset_id != second.dataset_id
