"""Tools module for common resources / shared code and "utilities" in the tm2py package."""
import multiprocessing
import re
from typing import Union


def parse_num_processors(value: Union[str, int, float]):
    """Convert input value (parse if string) to number of processors.
    Args:
        value: an int, float or string; string value can be "X" or "MAX-X"
    Returns:
        An int of the number of processors to use

    Raises:
        Exception: Input value exceeds number of available processors
        Exception: Input value less than 1 processors
    """
    max_processors = multiprocessing.cpu_count()
    if isinstance(value, str):
        result = value.upper()
        if result == "MAX":
            return max_processors
        if re.match("^[0-9]+$", value):
            return int(value)
        result = re.split(r"^MAX[\s]*-[\s]*", result)
        if len(result) == 2:
            return max(max_processors - int(result[1]), 1)
        raise Exception(f"Input value {value} is an int or string as 'MAX-X'")

    result = int(value)
    if result > max_processors:
        raise Exception(f"Input value {value} greater than available processors")
    if result < 1:
        raise Exception(f"Input value {value} less than 1 processors")
    return value
