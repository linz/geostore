from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from os import urandom
from random import choice, randrange
from string import ascii_letters, ascii_uppercase, digits, printable
from typing import Type
from uuid import uuid4

from mypy_boto3_lambda.type_defs import ResponseMetadataTypeDef

REFERENCE_DATETIME = datetime(2000, 1, 1, tzinfo=timezone.utc)


# General-purpose generators


def random_string(length: int) -> str:
    """
    Includes ASCII printable characters and the first printable character from several Unicode
    blocks <https://en.wikipedia.org/wiki/List_of_Unicode_characters>.
    """
    return _random_string_choices(f"{printable}¡ĀƀḂəʰͰἀЀ–⁰₠℀⅐←∀⌀①─▀■☀🬀✁ㄅﬀ", length)


def random_ascii_letter_string(length: int) -> str:
    return _random_string_choices(ascii_letters, length)


def _random_string_choices(characters: str, length: int) -> str:
    return "".join(choice(characters) for _ in range(length))


def any_past_datetime() -> datetime:
    return REFERENCE_DATETIME - timedelta(seconds=randrange(30_000_000_000))  # Back to year 1049


def any_past_datetime_string() -> str:
    return any_past_datetime().isoformat()


def any_past_utc_datetime_string() -> str:
    return any_past_datetime().strftime("%Y-%m-%dT%H:%M:%SZ")


def any_program_name() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_safe_file_path() -> str:
    paths = [any_safe_filename() for _ in range(randrange(1, 5))]
    return "/".join(paths)


def any_safe_filename() -> str:
    return _random_string_choices(f"{digits}{ascii_letters}", 20)


def any_host() -> str:
    return random_ascii_letter_string(20)


def any_https_url() -> str:
    host = any_host()
    path = any_safe_file_path()
    return f"https://{host}/{path}"


def any_file_contents(byte_count: int = 10) -> bytes:
    return urandom(byte_count)


def any_request_id() -> str:
    """Arbitrary-length string"""
    return uuid4().hex


def any_http_status_code() -> int:
    return choice(list(HTTPStatus))


def any_retry_attempts() -> int:
    """Arbitrary-length integer"""
    return randrange(10)


def any_response_metadata() -> ResponseMetadataTypeDef:
    return {
        "RequestId": any_request_id(),
        "HostId": any_host(),
        "HTTPStatusCode": any_http_status_code(),
        "HTTPHeaders": {},
        "RetryAttempts": any_retry_attempts(),
    }


def any_error_message() -> str:
    """Arbitrary-length string"""
    return random_string(50)


def any_class_name() -> str:
    return f"{choice(ascii_uppercase)}{random_ascii_letter_string(10)}Error"


def any_exception_class() -> Type[Exception]:
    exception_class = type(any_class_name(), (Exception,), {})
    return exception_class


def any_dictionary_key() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_etag() -> str:
    """Arbitrary-length string"""
    return random_string(10)


def any_name() -> str:
    return random_string(10)


def any_description() -> str:
    return random_string(20)
