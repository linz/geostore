from logging import NOTSET, Logger, StreamHandler, getLogger
from os import environ


def set_up_logging(name: str) -> Logger:
    logger = getLogger(name)

    log_handler = StreamHandler()
    log_level = environ.get("LOGLEVEL", NOTSET)

    logger.addHandler(log_handler)
    logger.setLevel(log_level)

    return logger
