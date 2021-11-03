import sys
from enum import IntEnum
from http import HTTPStatus
from json import dumps, load

import boto3
from botocore.exceptions import NoCredentialsError
from typer import Option, Typer, secho
from typer.colors import GREEN, RED, YELLOW

from .api_keys import MESSAGE_KEY
from .aws_keys import BODY_KEY, HTTP_METHOD_KEY
from .resources import ResourceName
from .step_function_keys import DATASET_ID_SHORT_KEY, DESCRIPTION_KEY, TITLE_KEY

app = Typer()
dataset_app = Typer()
app.add_typer(dataset_app, name="dataset")


class ExitCode(IntEnum):
    SUCCESS = 0
    UNKNOWN = 1
    # Exit code 2 is used by Typer to indicate usage error
    CONFLICT = 3
    NO_CREDENTIALS = 4


@dataset_app.command()
def create(title: str = Option(...), description: str = Option(...)) -> None:
    try:
        client = boto3.client("lambda")
    except NoCredentialsError:
        secho(
            "Unable to locate credentials. Make sure to log in to AWS first.", err=True, fg=YELLOW
        )
        sys.exit(ExitCode.NO_CREDENTIALS)

    request_object = {
        HTTP_METHOD_KEY: "POST",
        BODY_KEY: {TITLE_KEY: title, DESCRIPTION_KEY: description},
    }
    request_payload = dumps(request_object).encode()

    response = client.invoke(
        FunctionName=ResourceName.DATASETS_ENDPOINT_FUNCTION_NAME.value, Payload=request_payload
    )
    response_payload = load(response["Payload"])
    exit_code = {HTTPStatus.CREATED: ExitCode.SUCCESS, HTTPStatus.CONFLICT: ExitCode.CONFLICT}.get(
        response_payload["status_code"], ExitCode.UNKNOWN
    )
    color = {ExitCode.SUCCESS: GREEN, ExitCode.UNKNOWN: RED}.get(exit_code, YELLOW)
    response_body = response_payload[BODY_KEY]
    output = response_body.get(
        DATASET_ID_SHORT_KEY, response_body.get(MESSAGE_KEY, dumps(response_body))
    )

    secho(output, err=exit_code != ExitCode.SUCCESS, fg=color)

    sys.exit(exit_code)


if __name__ == "__main__":
    app()
