"""
TODO: add docstring here.

"""


def lambda_handler(event, _context):  # pylint:disable=unused-argument
    """Main Lambda entry point."""

    total_size = 100
    iteration_size = 10

    if "content" in event.keys():
        first_item = int(event["content"]["next_item"])
    else:
        first_item = 0

    if (first_item + iteration_size) <= total_size:
        next_item = first_item + iteration_size
    else:
        next_item = -1

    resp = {}

    # "first_item" value must be string. It is directly passed as value to Batch job environment
    # variable BATCH_JOB_FIRST_ITEM_INDEX. All environment variables must be string and there is no
    # chance of conversion.
    resp["first_item"] = str(first_item)

    resp["next_item"] = next_item
    resp["iteration_size"] = iteration_size

    return resp
