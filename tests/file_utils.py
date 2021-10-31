from io import BytesIO
from json import dumps
from typing import BinaryIO

from geostore.types import JsonObject


def json_dict_to_file_object(value: JsonObject) -> BinaryIO:
    return BytesIO(initial_bytes=dumps(value).encode())
