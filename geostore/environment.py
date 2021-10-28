from os import environ

ENV_NAME_VARIABLE_NAME = "GEOSTORE_ENV_NAME"


def environment_name() -> str:
    return environ.get(ENV_NAME_VARIABLE_NAME, "test")
