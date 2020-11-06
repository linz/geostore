"""Utility functions."""

from http.client import responses as http_responses


def error_response(code, message):
    """Return error response content as string."""

    return {"statusCode": code, "body": {"message": f"{http_responses[code]}: {message}."}}


def success_response(code, body):
    """Return success response content as string."""

    return {"statusCode": code, "body": body}
