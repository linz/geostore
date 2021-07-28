from typing import Tuple
from urllib.parse import urlparse


def get_bucket_and_key_from_url(url: str) -> Tuple[str, str]:
    parsed = urlparse(url)
    return parsed.netloc, parsed.path[1:]
