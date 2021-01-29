import logging
import sys
from argparse import Namespace
from unittest.mock import patch

from ..processing.check_stac_metadata.task import main
from .utils import any_program_name, any_s3_url


@patch("datalake.backend.processing.check_stac_metadata.task.STACSchemaValidator.validate")
def test_should_log_arguments(validate_url_mock) -> None:
    validate_url_mock.return_value = None
    url = any_s3_url()
    sys.argv = [any_program_name(), f"--metadata-url={url}"]
    logger = logging.getLogger("datalake.backend.processing.check_stac_metadata.task")

    with patch.object(logger, "debug") as logger_mock:
        main()

        logger_mock.assert_called_once_with(Namespace(metadata_url=url))
