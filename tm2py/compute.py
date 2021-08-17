import re
import multiprocessing
from typing import Union


def parse_num_processors(value: Union[str, int, float]):
    """Parse input value string "MAX-X" to number of available processors.
    Does not raise any specific errors.
    Args:
        value: int, float or string; string value can be "X" or "MAX-X"
    """
    max_processors = multiprocessing.cpu_count()
    if isinstance(value, str):
        value = value.upper()
        if value == "MAX":
            return max_processors
        if re.match("^[0-9]+$", value):
            return int(value)
        result = re.split(r"^MAX[\s]*-[\s]*", value)
        if len(result) == 2:
            return max(max_processors - int(result[1]), 1)
    else:
        return int(value)
    return value
