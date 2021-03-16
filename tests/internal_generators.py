from random import choice

from backend.check import Check


def any_check_name() -> Check:
    return choice(list(Check))
