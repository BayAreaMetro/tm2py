"""Base of tm2py module."""
from ._version import __version__
from .components.component import Component
from .config import (
    Configuration,
    HouseholdConfig,
    RunConfig,
    ScenarioConfig,
    TimePeriodConfig,
)
from .controller import RunController
from .examples import get_example
from .logger import Logger, LogStartEnd
from .setup_model.setup import SetupModel

__all__ = [
    # component
    "Component",
    # config
    "Configuration",
    "get_example",
    "HouseholdConfig",
    "RunConfig",
    "ScenarioConfig",
    "TimePeriodConfig",
    # controller
    "RunController",
    # setupmodel
    "SetupModel",
    # logger
    "Logger",
    "LogStartEnd",
]
