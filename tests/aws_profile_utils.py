from random import choice

from botocore.args import LEGACY_GLOBAL_STS_REGIONS


def any_region_name() -> str:
    region_name: str = choice(LEGACY_GLOBAL_STS_REGIONS)
    return region_name
