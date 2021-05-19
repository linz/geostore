import string
from datetime import datetime, timedelta, timezone
from os import urandom
from random import choice, randrange

REFERENCE_DATETIME = datetime(2000, 1, 1, tzinfo=timezone.utc)


# General-purpose generators


def random_string(length: int) -> str:
    """
    Includes ASCII printable characters and the first printable character from several Unicode
    blocks <https://en.wikipedia.org/wiki/List_of_Unicode_characters>.
    """
    return _random_string_choices(f"{string.printable}¡ĀƀḂəʰͰἀЀ–⁰₠℀⅐←∀⌀①─▀■☀🬀✁ㄅﬀ", length)


def random_ascii_letter_string(length: int) -> str:
    return _random_string_choices(string.ascii_letters, length)


def _random_string_choices(characters: str, length: int) -> str:
    return "".join(choice(characters) for _ in range(length))


def any_past_datetime() -> datetime:
    return REFERENCE_DATETIME - timedelta(seconds=randrange(30_000_000_000))  # Back to year 1049


def any_past_datetime_string() -> str:
    return any_past_datetime().isoformat()


def any_program_name() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_safe_file_path() -> str:
    paths = [any_safe_filename() for _ in range(randrange(1, 5))]
    return "/".join(paths)


def any_safe_filename() -> str:
    return _random_string_choices(f"{string.digits}{string.ascii_letters}", 20)


def any_host() -> str:
    return random_ascii_letter_string(20)


def any_https_url() -> str:
    host = any_host()
    path = any_safe_file_path()
    return f"https://{host}/{path}"


def any_file_contents() -> bytes:
    """Arbitrary-length bytes"""
    return urandom(20)


def any_error_message() -> str:
    """Arbitrary-length string"""
    return random_string(50)


def any_dictionary_key() -> str:
    """Arbitrary-length string"""
    return random_string(20)


def any_etag() -> str:
    """Arbitrary-length string"""
    return random_string(10)
