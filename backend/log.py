import logging
import os


def set_up_logging(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    log_handler = logging.StreamHandler()
    log_level = os.environ.get("LOGLEVEL", logging.NOTSET)

    logger.addHandler(log_handler)
    logger.setLevel(log_level)

    return logger
