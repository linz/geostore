from jsonschema import ValidationError  # type: ignore[import]
from pytest import raises

from ..processing.content_iterator.task import lambda_handler
from .utils import any_lambda_context


def test_should_raise_exception_if_event_is_missing_content() -> None:
    with raises(ValidationError):
        lambda_handler({}, any_lambda_context())
