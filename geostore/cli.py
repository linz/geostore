import sys
from enum import IntEnum
from http import HTTPStatus
from json import dumps, load
from typing import Callable, Optional, Union

import boto3
from botocore.exceptions import NoCredentialsError, NoRegionError
from typer import Option, Typer, secho
from typer.colors import GREEN, RED, YELLOW

from .api_keys import MESSAGE_KEY
from .aws_keys import BODY_KEY, HTTP_METHOD_KEY, STATUS_CODE_KEY
from .dataset_keys import DATASET_KEY_SEPARATOR
from .resources import ResourceName
from .step_function_keys import DATASET_ID_SHORT_KEY, DESCRIPTION_KEY, TITLE_KEY
from .types import JsonList, JsonObject

app = Typer()
dataset_app = Typer()
app.add_typer(dataset_app, name="dataset")

GetOutputFunctionType = Union[Callable[[JsonList], str], Callable[[JsonObject], str]]


class ExitCode(IntEnum):
    SUCCESS = 0
    UNKNOWN = 1
    # Exit code 2 is used by Typer to indicate usage error
    CONFLICT = 3
    NO_CREDENTIALS = 4
    NO_REGION_SETTING = 5


@dataset_app.command(name="create")
def dataset_create(title: str = Option(...), description: str = Option(...)) -> None:
    request_object = {
        HTTP_METHOD_KEY: "POST",
        BODY_KEY: {TITLE_KEY: title, DESCRIPTION_KEY: description},
    }

    def get_output(response_body: JsonObject) -> str:
        dataset_id: str = response_body[DATASET_ID_SHORT_KEY]
        return dataset_id

    handle_api_request(request_object, get_output)


@dataset_app.command(name="list")
def dataset_list(id_: Optional[str] = Option(None, "--id")) -> None:
    body = {}
    get_output: GetOutputFunctionType

    if id_ is None:

        def get_list_output(response_body: JsonList) -> str:
            lines = []
            for entry in response_body:
                lines.append(
                    f"{entry[TITLE_KEY]}{DATASET_KEY_SEPARATOR}{entry[DATASET_ID_SHORT_KEY]}"
                )
            return "\n".join(lines)

        get_output = get_list_output

    else:

        def get_single_output(response_body: JsonObject) -> str:
            return f"{response_body['title']}{DATASET_KEY_SEPARATOR}{response_body['id']}"

        body[DATASET_ID_SHORT_KEY] = id_
        get_output = get_single_output

    handle_api_request({HTTP_METHOD_KEY: "GET", BODY_KEY: body}, get_output)


@dataset_app.command(name="delete")
def dataset_delete(id_: str = Option(..., "--id")) -> None:
    handle_api_request({HTTP_METHOD_KEY: "DELETE", BODY_KEY: {DATASET_ID_SHORT_KEY: id_}}, None)


def handle_api_request(
    request_object: JsonObject, get_output: Optional[GetOutputFunctionType]
) -> None:
    response_payload = invoke_lambda(request_object)
    status_code = response_payload[STATUS_CODE_KEY]
    response_body = response_payload[BODY_KEY]

    if status_code in [HTTPStatus.OK, HTTPStatus.CREATED, HTTPStatus.NO_CONTENT]:
        if get_output is not None:
            output = get_output(response_body)
            secho(output, fg=GREEN)
        sys.exit(ExitCode.SUCCESS)

    if status_code == HTTPStatus.CONFLICT:
        secho(response_body[MESSAGE_KEY], err=True, fg=YELLOW)
        sys.exit(ExitCode.CONFLICT)

    secho(dumps(response_body), err=True, fg=RED)
    sys.exit(ExitCode.UNKNOWN)


def invoke_lambda(request_object: JsonObject) -> JsonObject:
    try:
        client = boto3.client("lambda")
    except NoRegionError:
        secho(
            "Unable to locate region settings. Make sure to log in to AWS first.",
            err=True,
            fg=YELLOW,
        )
        sys.exit(ExitCode.NO_REGION_SETTING)

    request_payload = dumps(request_object).encode()

    try:
        response = client.invoke(
            FunctionName=ResourceName.DATASETS_ENDPOINT_FUNCTION_NAME.value, Payload=request_payload
        )
    except NoCredentialsError:
        secho(
            "Unable to locate credentials. Make sure to log in to AWS first.", err=True, fg=YELLOW
        )
        sys.exit(ExitCode.NO_CREDENTIALS)

    response_payload: JsonObject = load(response["Payload"])
    return response_payload


if __name__ == "__main__":
    app()
