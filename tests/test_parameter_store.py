from json import dumps
from logging import getLogger
from unittest.mock import MagicMock, patch

from pytest import mark, raises

from backend import parameter_store
from backend.error_response_keys import ERROR_KEY
from backend.parameter_store import SSM_CLIENT, ParameterName, get_param


@mark.infrastructure
@patch(f"{parameter_store.__name__}.{ParameterName.__name__}")
def should_log_missing_parameter_name(parameter_name_mock: MagicMock) -> None:
    logger = getLogger(parameter_store.__name__)
    parameter_name = "invalid"
    parameter_name_mock.INVALID.value = parameter_name

    with patch.object(logger, "error") as logger_mock:
        with raises(SSM_CLIENT.exceptions.ParameterNotFound):
            get_param(parameter_name_mock.INVALID)

        logger_mock.assert_any_call(dumps({ERROR_KEY: f"Parameter not found: “{parameter_name}”"}))
