"""
Dataset Space Lambda handler function.
"""


def lambda_handler(event, context):  # pylint:disable=unused-argument
    """Main Lambda entry point."""

    return True


if __name__ == "__main__":
    lambda_handler({"x": "x", "y": "y"}, "context")
