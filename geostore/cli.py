import sys
from enum import IntEnum
from http import HTTPStatus
from json import dumps, load
from os import environ
from typing import Callable, Optional, Union

import boto3
from botocore.exceptions import NoCredentialsError, NoRegionError
from typer import Option, Typer, secho
from typer.colors import GREEN, RED, YELLOW

from .api_keys import MESSAGE_KEY
from .aws_keys import BODY_KEY, HTTP_METHOD_KEY, STATUS_CODE_KEY
from .dataset_properties import DATASET_KEY_SEPARATOR, TITLE_CHARACTERS
from .environment import ENV_NAME_VARIABLE_NAME, PRODUCTION_ENVIRONMENT_NAME
from .resources import Resource
from .step_function_keys import (
    DATASET_ID_SHORT_KEY,
    DESCRIPTION_KEY,
    EXECUTION_ARN_KEY,
    METADATA_URL_KEY,
    S3_ROLE_ARN_KEY,
    TITLE_KEY,
    VERSION_ID_KEY,
)
from .types import JsonList, JsonObject

DATASET_ID_HELP = "Dataset ID, as printed when running `geostore dataset create`."

HTTP_METHOD_CREATE = "POST"
HTTP_METHOD_RETRIEVE = "GET"

app = Typer(context_settings=dict(max_content_width=sys.maxsize))
dataset_app = Typer(help="Operate on entire datasets.")
dataset_version_app = Typer(help="Operate on dataset versions.")
app.add_typer(dataset_app, name="dataset")
app.add_typer(dataset_version_app, name="version")

GetOutputFunctionType = Union[Callable[[JsonList], str], Callable[[JsonObject], str]]


class ExitCode(IntEnum):
    SUCCESS = 0
    UNKNOWN = 1
    # Exit code 2 is used by Typer to indicate usage error
    CONFLICT = 3
    NO_CREDENTIALS = 4
    NO_REGION_SETTING = 5


@app.callback()
def main(
    environment_name: Optional[str] = Option(
        None,
        help="Set environment name, such as 'test'."
        f" Overrides the value of ${ENV_NAME_VARIABLE_NAME}."
        f"  [default: {PRODUCTION_ENVIRONMENT_NAME}]",
    )
) -> None:
    if environment_name:
        environ[ENV_NAME_VARIABLE_NAME] = environment_name
    elif ENV_NAME_VARIABLE_NAME not in environ:
        environ[ENV_NAME_VARIABLE_NAME] = PRODUCTION_ENVIRONMENT_NAME


@dataset_app.command(name="create", help="Create a new dataset.")
def dataset_create(
    title: str = Option(..., help=f"Allowed characters: '{TITLE_CHARACTERS}'."),
    description: str = Option(...),
) -> None:
    request_object = {
        HTTP_METHOD_KEY: HTTP_METHOD_CREATE,
        BODY_KEY: {TITLE_KEY: title, DESCRIPTION_KEY: description},
    }

    def get_output(response_body: JsonObject) -> str:
        dataset_id: str = response_body[DATASET_ID_SHORT_KEY]
        return dataset_id

    handle_api_request(
        Resource.DATASETS_ENDPOINT_FUNCTION_NAME.resource_name, request_object, get_output
    )


@dataset_app.command(name="list", help="List datasets.")
def dataset_list(id_: Optional[str] = Option(None, "--id", help=DATASET_ID_HELP)) -> None:
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

    handle_api_request(
        Resource.DATASETS_ENDPOINT_FUNCTION_NAME.resource_name,
        {HTTP_METHOD_KEY: HTTP_METHOD_RETRIEVE, BODY_KEY: body},
        get_output,
    )


@dataset_app.command(name="delete", help="Delete a dataset.")
def dataset_delete(id_: str = Option(..., "--id", help=DATASET_ID_HELP)) -> None:
    handle_api_request(
        Resource.DATASETS_ENDPOINT_FUNCTION_NAME.resource_name,
        {HTTP_METHOD_KEY: "DELETE", BODY_KEY: {DATASET_ID_SHORT_KEY: id_}},
        None,
    )


@dataset_version_app.command(name="create", help="Create a dataset version.")
def dataset_version_create(
    dataset_id: str = Option(..., help=DATASET_ID_HELP),
    metadata_url: str = Option(
        ...,
        help="S3 URL to the top level metadata file,"
        " for example 's3://my-bucket/my-dataset/collection.json'.",
    ),
    s3_role_arn: str = Option(
        ...,
        help="ARN of the role which the Geostore should assume to read your dataset,"
        " for example 'arn:aws:iam::1234567890:role/s3-reader'.",
    ),
) -> None:
    def get_output(response_body: JsonObject) -> str:
        return f"{response_body[VERSION_ID_KEY]}\t{response_body[EXECUTION_ARN_KEY]}"

    handle_api_request(
        Resource.DATASET_VERSIONS_ENDPOINT_FUNCTION_NAME.resource_name,
        {
            HTTP_METHOD_KEY: HTTP_METHOD_CREATE,
            BODY_KEY: {
                DATASET_ID_SHORT_KEY: dataset_id,
                METADATA_URL_KEY: metadata_url,
                S3_ROLE_ARN_KEY: s3_role_arn,
            },
        },
        get_output,
    )


@dataset_version_app.command(name="status", help="Get status of dataset version creation.")
def dataset_version_status(
    execution_arn: str = Option(
        ..., help="Execution ARN, as printed when running `geostore version create`."
    )
) -> None:
    def get_output(response_body: JsonObject) -> str:
        return dumps(response_body)

    handle_api_request(
        Resource.IMPORT_STATUS_ENDPOINT_FUNCTION_NAME.resource_name,
        {HTTP_METHOD_KEY: HTTP_METHOD_RETRIEVE, BODY_KEY: {EXECUTION_ARN_KEY: execution_arn}},
        get_output,
    )


def handle_api_request(
    function_name: str, request_object: JsonObject, get_output: Optional[GetOutputFunctionType]
) -> None:
    response_payload = invoke_lambda(function_name, request_object)
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


def invoke_lambda(function_name: str, request_object: JsonObject) -> JsonObject:
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
        response = client.invoke(FunctionName=function_name, Payload=request_payload)
    except NoCredentialsError:
        secho(
            "Unable to locate credentials. Make sure to log in to AWS first.", err=True, fg=YELLOW
        )
        sys.exit(ExitCode.NO_CREDENTIALS)

    response_payload: JsonObject = load(response["Payload"])
    return response_payload


if __name__ == "__main__":
    app()
