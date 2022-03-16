"""Config implementation and schema.
"""
# pylint: disable=too-many-instance-attributes

from abc import ABC
from dataclasses import dataclass, field, fields as _get_fields
from typing import List, Tuple, Union

import toml


# __BANNED_KEYS = ["items", "get", "_validate"]


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

    Note that datatypes must always have a single declared type, no use of Union or
    Any types.

    Not to be constructed directly. To be used a mixin for dataclasses
    representing config schema.

    Implement _validate method to add additional validation steps, such as values
    in right range, or conditional dependencies between items.

    Do not use any defined method ("get", "items", "_validate") for key names.

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
            if value is _MISSING:
                if field.default is _MISSING:
                    raise Exception(f"missing key {field.name}")
                self.__dict__[field.name] = field.default
                continue
            try:
                atype = get_type(field.type)
                value = atype(value)
                if isinstance(value, tuple):
                    item_atype = get_type(field.type.__args__[0])
                    processed_items = []
                    for i, item in enumerate(value):
                        try:
                            processed_items.append(item_atype(item))
                        except Exception as error:
                            raise Exception(f"[{i}]: {error}") from error
                    value = tuple(processed_items)
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

    def _validate(self):
        assert self.year >= 2005, "year must be at least 2005"


@dataclass(init=False, frozen=True)
class Run(ConfigItem):
    """Model run parameters"""

    start_iteration: int
    end_iteration: int
    start_component: str
    initial_components: Tuple[str]
    global_iteration_components: Tuple[str]
    final_components: Tuple[str]


@dataclass(init=False, frozen=True)
class TimePeriod(ConfigItem):
    """Time _time_period entry"""

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
    access_modes: Tuple[str]


@dataclass(init=False, frozen=True)
class AirPassenger(ConfigItem):
    """Air passenger model parameters"""

    highway_demand_file: str
    input_demand_folder: str
    reference_start_year: str
    reference_end_year: str
    demand_aggregation: Tuple[AirPassengerDemandAggregation]


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
    output: str
    max_dist_miles: float = None


@dataclass(init=False, frozen=True)
class ActiveModes(ConfigItem):
    """Active Mode skim parameters"""

    emme_scenario_id: int
    shortest_path_skims: Tuple[ActiveModeShortestPathSkim]


@dataclass(init=False, frozen=True)
class HighwayCapClass(ConfigItem):
    """Highway link capacity and speed ('capclass') index entry"""

    capclass: int
    capacity: float
    free_flow_speed: float
    critical_speed: float

    def _validate(self):
        assert self.capclass >= 0
        assert self.capacity >= 0
        assert self.free_flow_speed >= 0
        assert self.critical_speed >= 0


@dataclass(init=False, frozen=True)
class HighwayClassDemand(ConfigItem):
    """Highway class input source for demand"""

    source: str
    name: str
    factor: float = 1.0

    def _validate(self):
        valid_demand_components = [
            "household",
            "air_passenger",
            "internal_external",
            "truck",
        ]
        assert (
            self.source in valid_demand_components
        ), f"source must be one of {', '.join(valid_demand_components)}"
        assert self.factor > 0


@dataclass(init=False, frozen=True)
class HighwayClass(ConfigItem):
    """Highway assignment class definition"""

    name: str
    description: str
    mode_code: str
    excluded_links: Tuple[str]
    value_of_time: float
    operating_cost_per_mile: float
    skims: Tuple[str]
    demand: Tuple[HighwayClassDemand]
    toll: List[str] = field(default_factory=list)
    toll_factor: float = None
    pce: float = 1.0

    def _validate(self):
        assert len(self.name) <= 10, "name: maximum of 10 characters"
        assert len(self.mode_code) == 1, "mode_code: must be exactly 1 character"
        assert self.value_of_time > 0
        assert self.operating_cost_per_mile >= 0
        # list of skims validated under Highway to match toll dst group names
        # toll attribute expression validated under Highway to match toll dst group names
        assert self.toll_factor is None or self.toll_factor > 0
        assert self.pce > 0


@dataclass(init=False, frozen=True)
class HighwayTolls(ConfigItem):
    """Highway assignment and skim input tolls and related parameters"""

    file_path: str
    src_vehicle_group_names: Tuple[str]
    dst_vehicle_group_names: Tuple[str]
    tollbooth_start_index: int

    def _validate(self):
        assert len(self.src_vehicle_group_names) == len(
            self.dst_vehicle_group_names
        ), "dst_vehicle_group_names: must have number of items as src_vehicle_group_names"


@dataclass(init=False, frozen=True)
class DemandCountyGroup(ConfigItem):
    """Grouping of counties for assignment and demand files"""

    number: int
    counties: Tuple[str]

    def _validate(self):
        avialable_counties = [
            "San Francisco",
            "San Mateo",
            "Santa Clara",
            "Alameda",
            "Contra Costa",
            "Solano",
            "Napa",
            "Sonoma",
            "Marin",
        ]
        extra_counties = set(self.counties) - set(avialable_counties)
        assert not extra_counties, (
            f"counties: unrecognized names {','.join(extra_counties)} - "
            f"available counties are {','.join(avialable_counties)}"
        )


@dataclass(init=False, frozen=True)
class HighwayMazToMaz(ConfigItem):
    """Highway MAZ to MAZ shortest path assignment and skim parameters"""

    mode_code: str
    excluded_links: Tuple[str]
    operating_cost_per_mile: float
    value_of_time: float
    output_skim_file: str
    skim_period: str
    max_skim_cost: float
    demand_file: str
    demand_county_groups: Tuple[DemandCountyGroup]

    def _validate(self):
        assert len(self.mode_code) == 1, "mode_code: must be exactly 1 character"
        assert self.operating_cost_per_mile > 0
        assert self.value_of_time > 0
        # skim_period validated under top-level config
        assert self.max_skim_cost > 0
        group_ids = [group.number for group in self.demand_county_groups]
        assert len(group_ids) == len(
            set(group_ids)
        ), "demand_county_groups: number must be unique in list"


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
    capclass_lookup: Tuple[HighwayCapClass]
    classes: Tuple[HighwayClass]

    def _validate(self):
        assert self.relative_gap >= 0
        assert self.max_iterations >= 0
        error_msg = "generic_highway_mode_code: must be exactly 1 character"
        assert len(self.generic_highway_mode_code) == 1, error_msg
        assert self.area_type_buffer_dist_miles > 0
        capclass_ids = [i.capclass for i in self.capclass_lookup]
        error_msg = "capclass_lookup: capclass value must be unique in list"
        assert len(capclass_ids) == len(set(capclass_ids)), error_msg
        # validate class unique items
        class_names = [highway_class.name for highway_class in self.classes]
        error_msg = "classes: names must be unique"
        assert len(class_names) == len(set(class_names)), error_msg
        # validate class skim name list and toll attribute against toll setup
        # also if any mode IDs are used twice, that they have the same excluded links sets
        avail_skims = ["time", "dist", "hovdist", "tolldist", "freeflowtime"]
        available_link_sets = ["is_sr", "is_sr2", "is_sr3", "is_auto_only"]
        avail_toll_attrs = []
        for name in self.tolls.dst_vehicle_group_names:
            toll_types = [f"bridgetoll_{name}", f"valuetoll_{name}"]
            avail_skims.extend(toll_types)
            avail_toll_attrs.extend(["@" + name for name in toll_types])
            available_link_sets.append(f"is_toll_{name}")

        def check_keywords(class_num, key, value, available):
            extra_keys = set(value) - set(available)
            error_msg = (
                f"classes: [{class_num}]: {key}: unrecognized {key} name(s): "
                f"{','.join(extra_keys)}.  Available are: {', '.join(available)}"
            )
            assert not extra_keys, error_msg

        mode_excluded_links = {self.generic_highway_mode_code: set([])}
        for i, highway_class in enumerate(self.classes):
            check_keywords(i, "skim", highway_class.skims, avail_skims)
            check_keywords(i, "toll", highway_class.toll, avail_toll_attrs)
            check_keywords(
                i, "excluded_links", highway_class.excluded_links, available_link_sets
            )
            # maz_to_maz.mode_code must be unique
            assert (
                highway_class.mode_code != self.maz_to_maz.mode_code
            ), f"classes: [{i}]: mode_code: cannot be the same as the highway.maz_to_maz.mode_code"
            # make sure that if any mode IDs are used twice, they have the same excluded links sets
            if highway_class.mode_code in mode_excluded_links:
                ex_links1 = highway_class.excluded_links
                ex_links2 = mode_excluded_links[highway_class.mode_code]
                error_msg = (
                    f"classes: [{i}]: duplicated mode codes ('{highway_class.mode_code}') "
                    f"with different excluded links: {ex_links1} and {ex_links2}"
                )
                assert ex_links1 == ex_links2, error_msg
            mode_excluded_links[highway_class.mode_code] = highway_class.excluded_links


@dataclass(init=False, frozen=True)
class TransitMode(ConfigItem):
    """Transit mode definition (see also mode in the Emme API)"""

    mode_id: str
    name: str
    type: str
    assign_type: str
    in_vehicle_perception_factor: float = None
    speed_miles_per_hour: float = None

    def _validate(self):
        assert len(self.mode_id) == 1, "mode_id must be one character"
        valid_types = ["WALK", "ACCESS", "EGRESS", "LOCAL", "PREMIUM"]
        assert (
            self.type in valid_types
        ), f"assign_type must be one of {', '.join(valid_types)}"
        valid_assign_types = ["TRANSIT", "AUX_TRANSIT"]
        assert (
            self.assign_type in valid_assign_types
        ), f"assign_type must be one of {', '.join(valid_assign_types)}"
        assert (
            self.assign_type != "TRANSIT"
            or self.in_vehicle_perception_factor is not None
        ), "in_vehicle_perception_factor must be specified for TRANSIT mode"
        assert (
            self.assign_type != "AUX_TRANSIT" or self.speed_miles_per_hour is not None
        ), "speed_miles_per_hour must be specified for AUX_TRANSIT mode"


@dataclass(init=False, frozen=True)
class TransitVehicle(ConfigItem):
    """Transit vehicle definition (see also transit vehicle in the Emme API)"""

    vehicle_id: int
    mode: str
    name: str
    auto_equivalent: float
    seated_capacity: int = None
    total_capacity: int = None


@dataclass(init=False, frozen=True)
class Transit(ConfigItem):
    """Transit assignment parameters"""

    modes: Tuple[TransitMode]
    vehicles: Tuple[TransitVehicle]

    apply_msa_demand: bool
    value_of_time: float
    effective_headway_source: str
    initial_wait_perception_factor: float
    transfer_wait_perception_factor: float
    walk_perception_factor: float
    initial_boarding_penalty: float
    transfer_boarding_penalty: float
    max_transfers: int
    output_skim_path: str
    fares_path: str
    fare_matrix_path: str
    fare_max_transfer_distance_miles: float
    use_fares: bool
    override_connector_times: bool
    input_connector_access_times_path: str = None
    input_connector_egress_times_path: str = None
    output_stop_usage_path: str = None


@dataclass(init=False, frozen=True)
class Emme(ConfigItem):
    """Emme-specific parameters"""

    num_processors: str
    all_day_scenario_id: int
    project_path: str
    highway_database_path: str
    active_database_paths: Tuple[str]
    transit_database_path: str


@dataclass(init=False, frozen=True)
class Configuration(ConfigItem):
    """Configuration: root of the model configuration loaded from .toml files(s)

    Args:
        path: a valid system path to a .toml file or list of the same
    """

    scenario: Scenario
    run: Run
    time_periods: Tuple[TimePeriod]
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

    def _validate(self):
        # validate highway.maz_to_maz.skim_period refers to a valid period
        time_period_names = set(time.name for time in self.time_periods)
        assert (
            self.highway.maz_to_maz.skim_period in time_period_names
        ), "highway: maz_to_maz: skim_period: unrecognized period name"


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
                raise Exception(f"duplicate keys in source .toml files: {path}")
        else:
            right[key] = left[key]
