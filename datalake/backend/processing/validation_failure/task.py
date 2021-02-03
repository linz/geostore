from typing import Any, MutableMapping

JSON_OBJECT = MutableMapping[str, Any]


def lambda_handler(_event: JSON_OBJECT, _context: bytes) -> JSON_OBJECT:
    """Main Lambda entry point."""

    return {}
