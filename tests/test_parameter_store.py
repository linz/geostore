from unittest.mock import MagicMock, call, patch

from pytest import mark, raises

from geostore import parameter_store
from geostore.parameter_store import (
    LOG_MESSAGE_PARAMETER_NOT_FOUND,
    SSM_CLIENT,
    ParameterName,
    get_param,
)


@mark.infrastructure
@patch(f"{parameter_store.__name__}.{ParameterName.__name__}")
def should_log_missing_parameter_name(parameter_name_mock: MagicMock) -> None:
    parameter_name = "invalid"
    parameter_name_mock.INVALID.value = parameter_name

    with patch(f"{parameter_store.__name__}.LOGGER.error") as logger_mock:
        with raises(SSM_CLIENT.exceptions.ParameterNotFound):
            get_param(parameter_name_mock.INVALID)

        expected_log_call = call(LOG_MESSAGE_PARAMETER_NOT_FOUND, parameter_value=parameter_name)
        logger_mock.assert_has_calls([expected_log_call])
