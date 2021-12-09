"""Config implementation and schema.


"""
# pylint: disable=too-many-instance-attributes

from abc import ABC
from dataclasses import dataclass, fields as _get_fields
from typing import List, Union

import toml

__BANNED_KEYS = ["items", "get"]


def __get_missing_sentinel():
    # get _MISSING_TYPE from dataclass in order to check for unspecified default values
    @dataclass
    class _Empty:
        no_default: int

    return _get_fields(_Empty(0))[0].default


_MISSING = __get_missing_sentinel()


class ConfigItem(ABC):
    """Base class interface to source model configuration from TOML files.

    Loads dataclass from dictionary kwargs with table structure validation
    and value type casting. Allow use of .X and ["X"] and .get("X") from configuration.

    Not to be constructed directly. To be used a mixin for dataclasses
    representing config schema.

    Args:
        kwargs: input dictionary loaded from one or more TOML files
    """

    def __init__(self, kwargs: dict):
        def get_type(otype):
            try:
                return otype.__origin__
            except AttributeError:
                return otype

        fields = _get_fields(self)
        extra_keys = set(kwargs.keys()) - set(f.name for f in fields)
        if extra_keys:
            raise Exception(f"unexpected key(s): {', '.join(extra_keys)}")
        for field in fields:
            value = kwargs.get(field.name, _MISSING)
            if value is _MISSING and field.default is _MISSING:
                raise Exception(f"missing key {field.name}")
            try:
                atype = get_type(field.type)
                value = atype(value)
                if isinstance(value, list):
                    value = list(map(get_type(field.type.__args__[0]), value))
                self.__dict__[field.name] = value
            except Exception as error:
                raise Exception(f"{field.name}: {error}") from error
        self._validate()

    def _validate(self):
        pass

    def __getitem__(self, key):
        return getattr(self, key)

    def items(self):
        """D.items() -> a set-like object providing a view on D's items"""
        return self.__dict__.items()

    def get(self, key, default=None):
        """Return the value for key if key is in the dictionary, else default."""
        return self.__dict__.get(key, default)


@dataclass(init=False, frozen=True)
class Scenario(ConfigItem):
    """Scenario parameters"""

    year: int
    verify: bool
    maz_landuse_file: str


@dataclass(init=False, frozen=True)
class Run(ConfigItem):
    """Model run parameters"""

    start_iteration: int
    end_iteration: int
    start_component: str
    initial_components: List[str]
    global_iteration_components: List[str]
    final_components: List[str]


@dataclass(init=False, frozen=True)
class TimePeriod(ConfigItem):
    """Time period entry"""

    name: str
    length_hours: float
    highway_capacity_factor: float
    emme_scenario_id: int


@dataclass(init=False, frozen=True)
class Household(ConfigItem):
    """Household (residents) model parameters"""

    highway_demand_file: str
    transit_demand_file: str


@dataclass(init=False, frozen=True)
class AirPassengerDemandAggregation(ConfigItem):
    """Air passenger demand aggregation input parameters"""

    result_class_name: str
    src_group_name: str
    access_modes: List[str]


@dataclass(init=False, frozen=True)
class AirPassenger(ConfigItem):
    """Air passenger model parameters"""

    highway_demand_file: str
    input_demand_folder: str
    reference_start_year: str
    reference_end_year: str
    demand_aggregation: List[AirPassengerDemandAggregation]


@dataclass(init=False, frozen=True)
class InternalExternal(ConfigItem):
    """Internal <-> External model parameters"""

    highway_demand_file: str
    input_demand_file: str
    reference_year: int
    toll_choice_time_coefficient: float
    value_of_time: float
    shared_ride_2_toll_factor: float
    shared_ride_3_toll_factor: float
    operating_cost_per_mile: float


@dataclass(init=False, frozen=True)
class Truck(ConfigItem):
    """Truck model parameters"""

    highway_demand_file: str
    k_factors_file: str
    friction_factors_file: str
    value_of_time: float
    operating_cost_per_mile: float
    toll_choice_time_coefficient: float
    max_balance_iterations: int
    max_balance_relative_error: float


@dataclass(init=False, frozen=True)
class ActiveModeShortestPathSkim(ConfigItem):
    """Active mode skim entry"""

    mode: str
    roots: str
    leaves: str
    max_dist_miles: float
    output: str


@dataclass(init=False, frozen=True)
class ActiveModes(ConfigItem):
    """Active Mode skim parameters"""

    emme_scenario_id: int
    shortest_path_skims: List[ActiveModeShortestPathSkim]


@dataclass(init=False, frozen=True)
class HighwayCapClass(ConfigItem):
    """Highway link capacity and speed ('capclass') index entry"""

    capclass: int
    capacity: float
    free_flow_speed: float
    critical_speed: float


@dataclass(init=False, frozen=True)
class HighwayClassDemand(ConfigItem):
    """Highway class input source for demand"""

    source: str
    name: str
    factor: float = 1.0


@dataclass(init=False, frozen=True)
class HighwayClass(ConfigItem):
    """Highway assignment class definition"""

    name: str
    description: str
    mode_code: str
    excluded_links: List[str]
    value_of_time: float
    operating_cost_per_mile: float
    skims: List[str]
    demand: List[HighwayClassDemand]
    toll: str
    toll_factor: float = None
    pce: float = 1.0

    def _validate(self):
        assert len(self.mode_code) == 1, "mode_code: must be exactly 1 character"


@dataclass(init=False, frozen=True)
class HighwayTolls(ConfigItem):
    """Highway assignment and skim input tolls and related parameters"""

    file_path: str
    src_vehicle_group_names: List[str]
    dst_vehicle_group_names: List[str]
    tollbooth_start_index: int


@dataclass(init=False, frozen=True)
class HighwayMazToMaz(ConfigItem):
    """Highway MAZ to MAZ shortest path assignment and skim parameters"""

    mode_code: str
    excluded_links: List[str]
    operating_cost_per_mile: float
    value_of_time: float
    input_maz_highway_demand_file: str
    output_skim_file: str
    skim_period: str
    max_skim_cost: float


@dataclass(init=False, frozen=True)
class Highway(ConfigItem):
    """Highway assignment and skims parameters"""

    output_skim_path: str
    relative_gap: float
    max_iterations: int
    generic_highway_mode_code: str
    area_type_buffer_dist_miles: float
    tolls: HighwayTolls
    maz_to_maz: HighwayMazToMaz
    capclass_lookup: List[HighwayCapClass]
    classes: List[HighwayClass]


@dataclass(init=False, frozen=True)
class TransitMode(ConfigItem):
    """Transit mode definition (see also mode in the Emme API)"""

    mode_id: str
    name: str
    type: str
    assign_type: str
    speed_miles_per_hour: float


@dataclass(init=False, frozen=True)
class TransitVehicle(ConfigItem):
    """Transit vehicle definition (see also transit vehicle in the Emme API)"""

    line_id: int
    mode: str
    name: str
    auto_equivalent: float
    seated_capacity: int
    total_capacity: int


@dataclass(init=False, frozen=True)
class Transit(ConfigItem):
    """Transit assignment parameters"""

    modes: List[TransitMode]
    vehicles: List[TransitVehicle]

    apply_msa_demand: bool
    value_of_time: float
    effective_headway_source: str
    initial_wait_perception_factor: float
    transfer_wait_perception_factor: float
    walk_perception_factor: float
    in_vehicle_perception_factor: float
    initial_boarding_penalty: float
    transfer_boarding_penalty: float
    max_transfers: int
    output_skim_path: str
    fares_path: str
    fare_matrix_path: str
    fare_max_transfer_distance_miles: float
    use_fares: bool
    override_connector_times: bool
    input_connector_access_times_path: str
    input_connector_egress_times_path: str
    output_stop_usage_path: str


@dataclass(init=False, frozen=True)
class Emme(ConfigItem):
    """Emme-specific parameters"""

    num_processors: str
    all_day_scenario_id: int
    project_path: str
    highway_database_path: str
    active_database_paths: str
    transit_database_path: str


@dataclass(init=False, frozen=True)
class Configuration(ConfigItem):
    """Configuration: root of the model configuration loaded from .toml files(s)

    Args:
        path: a valid system path to a .toml file or list of the same
    """

    scenario: Scenario
    run: Run
    time_periods: List[TimePeriod]
    household: Household
    air_passenger: AirPassenger
    internal_external: InternalExternal
    truck: Truck
    active_modes: ActiveModes
    highway: Highway
    transit: Transit
    emme: Emme

    def __init__(self, path: Union[str, List[str]]):
        if isinstance(path, str):
            path = [path]
        data = _load_toml(path[0])
        for path_item in path[1:]:
            _merge_dicts(data, _load_toml(path_item))
        super().__init__(data)


def _load_toml(path: str) -> dict:
    """Load config from toml file at path"""
    with open(path, "r", encoding="utf-8") as toml_file:
        data = toml.load(toml_file)
    return data


def _merge_dicts(right, left, path=None):
    """Merges the contents of nested dict left into nested dict right.

    Raises errors in case of namespace conflicts.
    Args:
        right: dict, modified in place
        left: dict to be merged into right
        path: default None, sequence of keys to be reported in case of
            error in merging nested dictionaries
    """
    if path is None:
        path = []
    for key in left:
        if key in right:
            if isinstance(right[key], dict) and isinstance(left[key], dict):
                _merge_dicts(right[key], left[key], path + [str(key)])
            else:
                path = ".".join(path + [str(key)])
                raise Exception(f"duplicate keys in source .toml files at {path}")
        else:
            right[key] = left[key]
