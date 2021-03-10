from ..types import JsonObject


def lambda_handler(_event: JsonObject, _context: bytes) -> JsonObject:
    """Main Lambda entry point."""

    resp = {"success": True, "message": ""}

    return resp
