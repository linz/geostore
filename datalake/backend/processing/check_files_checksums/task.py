#!/usr/bin/env python3
from typing import Callable

from botocore.response import StreamingBody  # type: ignore[import]
from multihash import FUNCS, decode  # type: ignore[import]

CHUNK_SIZE = 1024


def validate_url_multihash(
    url: str, hex_multihash: str, url_reader: Callable[[str], StreamingBody]
) -> bool:
    url_stream = url_reader(url)
    checksum_function_code = int(hex_multihash[:2], 16)
    checksum_function = FUNCS[checksum_function_code]

    file_digest = checksum_function()
    for chunk in url_stream.iter_chunks(chunk_size=CHUNK_SIZE):
        file_digest.update(chunk)

    actual_digest: bytes = file_digest.digest()
    expected_digest: bytes = decode(bytes.fromhex(hex_multihash))
    return actual_digest == expected_digest
