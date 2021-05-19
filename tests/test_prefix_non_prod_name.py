from os import environ
from unittest.mock import patch

from backend.environment import ENV_NAME_VARIABLE_NAME
from backend.resources import PRODUCTION_ENVIRONMENT_NAME, prefix_non_prod_name


def should_return_original_name_when_production() -> None:
    name = "any name"
    with patch.dict(environ, {ENV_NAME_VARIABLE_NAME: PRODUCTION_ENVIRONMENT_NAME}):
        assert prefix_non_prod_name(name) == name


def should_return_prefixed_name_when_not_production() -> None:
    name = "any name"
    environment_name = f"not {PRODUCTION_ENVIRONMENT_NAME}"
    with patch.dict(environ, {ENV_NAME_VARIABLE_NAME: environment_name}):
        assert prefix_non_prod_name(name) == f"{environment_name}-{name}"
