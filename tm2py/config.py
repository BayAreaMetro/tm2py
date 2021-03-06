"""Config implementation and schema.
"""
# pylint: disable=too-many-instance-attributes

from abc import ABC
from typing import List, Tuple, Union, Optional
from typing_extensions import Literal

from pydantic import Field, validator
from pydantic.dataclasses import dataclass
import toml


class ConfigItem(ABC):
    """Base class to add partial dict-like interface to tm2py model configuration.

    Allow use of .items() ["X"] and .get("X") .to_dict() from configuration.

    Not to be constructed directly. To be used a mixin for dataclasses
    representing config schema.
    Do not use "get" "to_dict", or "items" for key names.
    """

    def __getitem__(self, key):
        return getattr(self, key)

    def items(self):
        """D.items() -> a set-like object providing a view on D's items"""
        return self.__dict__.items()

    def get(self, key, default=None):
        """Return the value for key if key is in the dictionary, else default."""
        return self.__dict__.get(key, default)


@dataclass(frozen=True)
class ScenarioConfig(ConfigItem):
    """Scenario related parameters

    Properties:
        verify: optional, default False if specified as True components will run
            additional post-process verify step to validate results / outputs
            (not implemented yet)
        maz_landuse_file: relative path to maz_landuse_file used by multiple
            components
        year: model year, must be at least 2005
    """

    maz_landuse_file: str
    year: int = Field(ge=2005)
    verify: Optional[bool] = Field(default=False)


ComponentNames = Literal[
    "create_tod_scenarios",
    "active_modes",
    "air_passenger",
    "prepare_network_highway",
    "highway_maz_assign",
    "highway",
    "highway_maz_skim",
    "transit",
    "household",
    "visitor",
    "internal_external",
    "truck",
]
EmptyString = Literal[""]


@dataclass(frozen=True)
class RunConfig(ConfigItem):
    """Model run parameters

    Properties:
        start_iteration: start iteration number, 0 to include initial_components
        end_iteration: final iteration number
        start_component: name of component to start with, will skip components
            list prior to this component
        initial_components: list of components to run as initial (0) iteration
        global_iteration_components: list of component to run at every iteration, in order
        final_components: list of components to run after final iteration, in order
    """

    initial_components: Tuple[ComponentNames, ...]
    global_iteration_components: Tuple[ComponentNames, ...]
    final_components: Tuple[ComponentNames, ...]
    start_iteration: int = Field(ge=0)
    end_iteration: int = Field(gt=0)
    start_component: Optional[Union[ComponentNames, EmptyString]] = Field(default="")

    @classmethod
    @validator("end_iteration")
    def end_iteration_gt_start(cls, value, values):
        """Validate end_iteration greater than start_iteration"""
        if "start_iteration" in values:
            assert (
                value > values["start_iteration"]
            ), "must be greater than start_iteration"
        return value


@dataclass(frozen=True)
class TimePeriodConfig(ConfigItem):
    """Time time period entry"""

    name: str
    length_hours: float = Field(gt=0)
    highway_capacity_factor: float = Field(gt=0)
    emme_scenario_id: int = Field(ge=1)


@dataclass(frozen=True)
class HouseholdConfig(ConfigItem):
    """Household (residents) model parameters"""

    highway_demand_file: str
    transit_demand_file: str


@dataclass(frozen=True)
class AirPassengerDemandAggregationConfig(ConfigItem):
    """Air passenger demand aggregation input parameters"""

    result_class_name: str
    src_group_name: str
    access_modes: Tuple[str, ...]


@dataclass(frozen=True)
class AirPassengerConfig(ConfigItem):
    """Air passenger model parameters"""

    highway_demand_file: str
    input_demand_folder: str
    reference_start_year: str
    reference_end_year: str
    demand_aggregation: Tuple[AirPassengerDemandAggregationConfig, ...]


@dataclass(frozen=True)
class InternalExternalConfig(ConfigItem):
    """Internal <-> External model parameters"""

    highway_demand_file: str
    input_demand_file: str
    reference_year: int
    toll_choice_time_coefficient: float
    value_of_time: float
    shared_ride_2_toll_factor: float
    shared_ride_3_toll_factor: float
    operating_cost_per_mile: float


@dataclass(frozen=True)
class TruckConfig(ConfigItem):
    """Truck model parameters"""

    highway_demand_file: str
    k_factors_file: str
    friction_factors_file: str
    value_of_time: float
    operating_cost_per_mile: float
    toll_choice_time_coefficient: float
    max_balance_iterations: int
    max_balance_relative_error: float


@dataclass(frozen=True)
class ActiveModeShortestPathSkimConfig(ConfigItem):
    """Active mode skim entry"""

    mode: str
    roots: str
    leaves: str
    output: str
    max_dist_miles: float = None


@dataclass(frozen=True)
class ActiveModesConfig(ConfigItem):
    """Active Mode skim parameters"""

    emme_scenario_id: int
    shortest_path_skims: Tuple[ActiveModeShortestPathSkimConfig, ...]


@dataclass(frozen=True)
class HighwayCapClassConfig(ConfigItem):
    """Highway link capacity and speed ('capclass') index entry

    Properties:
        capclass: cross index for link @capclass lookup
        capacity: value for link capacity, PCE / hour
        free_flow_speed: value for link free flow speed, miles / hour
        critical_speed: value for cirtical speed (Ja) used in Akcelik
            type functions
    """

    capclass: int = Field(ge=0)
    capacity: float = Field(ge=0)
    free_flow_speed: float = Field(ge=0)
    critical_speed: float = Field(ge=0)


@dataclass(frozen=True)
class HighwayClassDemandConfig(ConfigItem):
    """Highway class input source for demand.

    Used to specify where to find related demand file for this
    highway class. Multiple

    Properties:
        source: reference name of the component section for the
                source "highway_demand_file" location, one of:
                "household", "air_passenger", "internal_external", "truck"
        name: name of matrix in the OMX file, can include "{period}"
                placeholder
        factor: optional, multiplicative factor to generate PCEs from
                trucks or convert person-trips to vehicle-trips for HOVs
    """

    name: str = Field()
    source: str = Literal["household", "air_passenger", "internal_external", "truck"]
    factor: float = Field(default=1.0, gt=0)


@dataclass(frozen=True)
class HighwayClassConfig(ConfigItem):
    """Highway assignment class definition.

    Note that excluded_links, skims and toll attribute names include
    vehicle groups ("{vehicle}") which reference the list of
    highway.toll.dst_vehicle_group_names (see HighwayTollsConfig).
    The default example model config uses:
    "da", "sr2", "sr3", "vsm", sml", "med", "lrg"

    Example single class config:
        name = "da"
        description= "drive alone"
        mode_code= "d"
        [[highway.classes.demand]]
            source = "household"
            name = "SOV_GP_{period}"
        [[highway.classes.demand]]
            source = "air_passenger"
            name = "da"
        [[highway.classes.demand]]
            source = "internal_external"
            name = "da"
        excluded_links = ["is_toll_da", "is_sr2"],
        value_of_time = 18.93,  # $ / hr
        operating_cost_per_mile = 17.23,  # cents / mile
        toll = ["@bridgetoll_da"]
        skims = ["time", "dist", "freeflowtime", "bridgetoll_da"],

    Properties:
        name: short (up to 10 character) unique reference name for the class.
            used in attribute and matrix names
        description: longer text used in attribute and matrix descriptions
        mode_code: single character mode, used to generate link.modes to
            identify subnetwork, generated from "excluded_links" keywords.
            Should be unique in list of classes, unless multiple classes
            have identical excluded_links specification. Cannot be the
            same as used for highway.maz_to_maz.mode_code.
        value_of_time: value of time for this class in $ / hr
        operating_cost_per_mile: vehicle operating cost in cents / mile
        demand: list of OMX file and matrix keyname references,
            see HighwayClassDemandConfig
        excluded_links: list of keywords to identify links to exclude from
            this class' available subnetwork (generate link.modes)
            Options are:
                - "is_sr": is reserved for shared ride (@useclass in 2,3)
                - "is_sr2": is reserved for shared ride 2+ (@useclass == 2)
                - "is_sr3": is reserved for shared ride 3+ (@useclass == 3)
                - "is_auto_only": is reserved for autos (non-truck) (@useclass != 1)
                - "is_toll_{vehicle}": has a value (non-bridge) toll for the {vehicle} toll group
        toll: list of additional toll cost link attribute (values stored in cents),
            summed, one of "@bridgetoll_{vehicle}", "@valuetoll_{vehicle}"
        toll_factor: optional, factor to apply to toll values in cost calculation
        pce: optional, passenger car equivalent to convert assigned demand in
            PCE units to vehicles for total assigned vehicle calculations
        skims: list of skim matrices to generate
            Options are:
                "time": pure travel time in minutes
                "dist": distance in miles
                "hovdist": distance on HOV facilities (is_sr2 or is_sr3)
                "tolldist": distance on toll facilities
                    (@tollbooth > highway.tolls.tollbooth_start_index)
                "freeflowtime": free flow travel time in minutes
                "bridgetoll_{vehicle}": bridge tolls, {vehicle} refers to toll group
                "valuetoll_{vehicle}": other, non-bridge tolls, {vehicle} refers to toll group
    """

    name: str = Field(min_length=1, max_length=10)
    description: str = Field(default="")
    mode_code: str = Field(min_length=1, max_length=1)
    value_of_time: float = Field(gt=0)
    operating_cost_per_mile: float = Field(ge=0)
    pce: Optional[float] = Field(default=1.0, gt=0)
    # Note that excluded_links, skims, and tolls validated under HighwayConfig to include
    # highway.toll.dst_vehicle_group_names names
    excluded_links: Tuple[str, ...] = Field()
    skims: Tuple[str, ...] = Field()
    toll: Tuple[str, ...] = Field()
    toll_factor: Optional[float] = Field(default=None, gt=0)
    demand: Tuple[HighwayClassDemandConfig, ...] = Field()


@dataclass(frozen=True)
class HighwayTollsConfig(ConfigItem):
    """Highway assignment and skim input tolls and related parameters.

    Properties:
        file_path: source relative file path for the highway tolls index CSV
        tollbooth_start_index: tollbooth separates links with "bridge" tolls
            (index < this value) vs. "value" tolls. These toll attributes
            can then be referenced separately in the highway.classes[].tolls
            list
        src_vehicle_group_names: name used for the vehicle toll CSV column IDs,
            of the form "toll{period}_{vehicle}"
        dst_vehicle_group_names: list of names used in destination network
            for the corresponding vehicle group. Length of list must be the same
            as src_vehicle_group_names. Used for toll related attributes and
            resulting skim matrices. Cross-referenced in list of highway.classes[]:
                excluded_links: "is_toll_{vehicle}"
                tolls: "@bridgetoll_{vehicle}", "@valuetoll_{vehicle}"
                skims: "bridgetoll_{vehicle}", "valuetoll_{vehicle}"
    """

    file_path: str = Field()
    tollbooth_start_index: int = Field(gt=1)
    src_vehicle_group_names: Tuple[str, ...] = Field()
    dst_vehicle_group_names: Tuple[str, ...] = Field()

    @classmethod
    @validator("dst_vehicle_group_names", always=True)
    def dst_vehicle_group_names_length(cls, value, values):
        """Validate dst_vehicle_group_names has same length as src_vehicle_group_names"""
        if "src_vehicle_group_names" in values:
            assert len(value) == len(
                values["src_vehicle_group_names"]
            ), "must be same length as src_vehicle_group_names"
        return value


COUNTY_NAMES = Literal[
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


@dataclass(frozen=True)
class DemandCountyGroupConfig(ConfigItem):
    """Grouping of counties for assignment and demand files

    Properties:
        number: id number for this group, must be unique
        counties: list of one or more county names
    """

    number: int = Field()
    counties: Tuple[COUNTY_NAMES, ...] = Field()


@dataclass(frozen=True)
class HighwayMazToMazConfig(ConfigItem):
    """Highway MAZ to MAZ shortest path assignment and skim parameters

    Properties:
        mode_code: single character mode, used to generate link.modes to
            identify subnetwork, generated from "excluded_links" keywords,
            plus including MAZ connectors.
        value_of_time: value of time for this class in $ / hr
        operating_cost_per_mile: vehicle operating cost in cents / mile
        max_skim_cost: max shortest path distance to search for MAZ-to-MAZ
            skims, in generized costs units (includes operating cost
            converted to minutes)
        excluded_links: list of keywords to identify links to exclude from
            MAZ-to-MAZ paths, see HighwayClassConfig.excluded_links
        demand_file: relative path to find the input demand files
            can have use a placeholder for {period} and {number}, where the
            {period} is the time_period.name (see TimePeriodConfig)
            and {number} is the demand_count_groups[].number
            (see DemandCountyGroupConfig)
            e.g.: auto_{period}_MAZ_AUTO_{number}_{period}.omx
        demand_county_groups: List of demand county names and
        skim_period: period name to use for the shotest path skims, must
            match one of the names listed in the time_periods
        output_skim_file: relative path to resulting MAZ-to-MAZ skims
    """

    mode_code: str = Field(min_length=1, max_length=1)
    value_of_time: float = Field(gt=0)
    operating_cost_per_mile: float = Field(ge=0)
    max_skim_cost: float = Field(gt=0)
    excluded_links: Tuple[str, ...] = Field()
    demand_file: str = Field()
    demand_county_groups: Tuple[DemandCountyGroupConfig, ...] = Field()
    skim_period: str = Field()
    output_skim_file: str = Field()

    @classmethod
    @validator("demand_county_groups")
    def unique_group_numbers(cls, value):
        """Validate list of demand_county_groups has unique .number values"""
        group_ids = [group.number for group in value]
        assert len(group_ids) == len(set(group_ids)), "-> number value must be unique"
        return value


@dataclass(frozen=True)
class HighwayConfig(ConfigItem):
    """Highway assignment and skims parameters

    Properties:
        generic_highway_mode_code: single character unique mode ID for entire
            highway network (no excluded_links)
        relative_gap: target relative gap stopping criteria
        max_iterations: maximum iterations stopping criteria
        area_type_buffer_dist_miles: used to in calculation to categorize link @areatype
            The area type is determined based on the average density of nearby
            (within this buffer distance) MAZs, using (pop+jobs*2.5)/acres
        output_skim_path: relative path template for output skims in OMX format
        tolls: input toll specification, see HighwayTollsConfig
        maz_to_maz: maz-to-maz shortest path assignment and skim specification,
            see HighwayMazToMazConfig
        classes: highway assignment multi-class setup and skim specification,
            see HighwayClassConfig
        capclass_lookup: index cross-reference table from the link @capclass value
            to the free-flow speed, capacity, and critical speed values
    """

    generic_highway_mode_code: str = Field(min_length=1, max_length=1)
    relative_gap: float = Field(ge=0)
    max_iterations: int = Field(ge=0)
    area_type_buffer_dist_miles: float = Field(gt=0)
    output_skim_path: str = Field()
    tolls: HighwayTollsConfig = Field()
    maz_to_maz: HighwayMazToMazConfig = Field()
    classes: Tuple[HighwayClassConfig, ...] = Field()
    capclass_lookup: Tuple[HighwayCapClassConfig, ...] = Field()

    @classmethod
    @validator("capclass_lookup")
    def unique_capclass_numbers(cls, value):
        """Validate list of capclass_lookup has unique .capclass values"""
        capclass_ids = [i.capclass for i in value]
        error_msg = "-> capclass value must be unique in list"
        assert len(capclass_ids) == len(set(capclass_ids)), error_msg
        return value

    @classmethod
    @validator("classes", pre=True)
    def unique_class_names(cls, value):
        """Validate list of classes has unique .name values"""
        class_names = [highway_class["name"] for highway_class in value]
        error_msg = "-> name value must be unique in list"
        assert len(class_names) == len(set(class_names)), error_msg
        return value

    @classmethod
    @validator("classes")
    def validate_class_mode_excluded_links(cls, value, values):
        """Validate list of classes has unique .mode_code or .excluded_links match"""
        # validate if any mode IDs are used twice, that they have the same excluded links sets
        mode_excluded_links = {values["generic_highway_mode_code"]: set([])}
        for i, highway_class in enumerate(value):
            # maz_to_maz.mode_code must be unique
            if "maz_to_maz" in values:
                assert (
                    highway_class["mode_code"] != values["maz_to_maz"]["mode_code"]
                ), f"-> {i} -> mode_code: cannot be the same as the highway.maz_to_maz.mode_code"
            # make sure that if any mode IDs are used twice, they have the same excluded links sets
            if highway_class.mode_code in mode_excluded_links:
                ex_links1 = highway_class["excluded_links"]
                ex_links2 = mode_excluded_links[highway_class["mode_code"]]
                error_msg = (
                    f"-> {i}: duplicated mode codes ('{highway_class['mode_code']}') "
                    f"with different excluded links: {ex_links1} and {ex_links2}"
                )
                assert ex_links1 == ex_links2, error_msg
            mode_excluded_links[highway_class.mode_code] = highway_class.excluded_links
        return value

    @classmethod
    @validator("classes")
    def validate_class_keyword_lists(cls, value, values):
        """Validate classes .skims, .toll, and .excluded_links values"""
        if "tolls" not in values:
            return value
        avail_skims = ["time", "dist", "hovdist", "tolldist", "freeflowtime"]
        available_link_sets = ["is_sr", "is_sr2", "is_sr3", "is_auto_only"]
        avail_toll_attrs = []
        for name in values["tolls"].dst_vehicle_group_names:
            toll_types = [f"bridgetoll_{name}", f"valuetoll_{name}"]
            avail_skims.extend(toll_types)
            avail_toll_attrs.extend(["@" + name for name in toll_types])
            available_link_sets.append(f"is_toll_{name}")

        # validate class skim name list and toll attribute against toll setup
        def check_keywords(class_num, key, val, available):
            extra_keys = set(val) - set(available)
            error_msg = (
                f" -> {class_num} -> {key}: unrecognized {key} name(s): "
                f"{','.join(extra_keys)}.  Available names are: {', '.join(available)}"
            )
            assert not extra_keys, error_msg

        for i, highway_class in enumerate(value):
            check_keywords(i, "skim", highway_class["skims"], avail_skims)
            check_keywords(i, "toll", highway_class["toll"], avail_toll_attrs)
            check_keywords(
                i,
                "excluded_links",
                highway_class["excluded_links"],
                available_link_sets,
            )
        return value


@dataclass(frozen=True)
class TransitModeConfig(ConfigItem):
    """Transit mode definition (see also mode in the Emme API)"""

    type: Literal["WALK", "ACCESS", "EGRESS", "LOCAL", "PREMIUM"]
    assign_type: Literal["TRANSIT", "AUX_TRANSIT"]
    mode_id: str = Field(min_length=1, max_length=1)
    name: str = Field(max_length=10)
    in_vehicle_perception_factor: Optional[float] = Field(default=None, ge=0)
    speed_miles_per_hour: Optional[float] = Field(default=None, gt=0)

    @classmethod
    @validator("in_vehicle_perception_factor", always=True)
    def in_vehicle_perception_factor_valid(cls, value, values):
        """Validate in_vehicle_perception_factor exists if assign_type is TRANSIT"""
        if "assign_type" in values and values["assign_type"] == "TRANSIT":
            assert value is not None, "must be specified when assign_type==TRANSIT"
        return value

    @classmethod
    @validator("speed_miles_per_hour", always=True)
    def speed_miles_per_hour_valid(cls, value, values):
        """Validate speed_miles_per_hour exists if assign_type is AUX_TRANSIT"""
        if "assign_type" in values and values["assign_type"] == "AUX_TRANSIT":
            assert value is not None, "must be specified when assign_type==AUX_TRANSIT"
        return value


@dataclass(frozen=True)
class TransitVehicleConfig(ConfigItem):
    """Transit vehicle definition (see also transit vehicle in the Emme API)"""

    vehicle_id: int
    mode: str
    name: str
    auto_equivalent: Optional[float] = Field(default=0, ge=0)
    seated_capacity: Optional[int] = Field(default=None, ge=0)
    total_capacity: Optional[int] = Field(default=None, ge=0)


@dataclass(frozen=True)
class TransitConfig(ConfigItem):
    """Transit assignment parameters"""

    modes: Tuple[TransitModeConfig, ...]
    vehicles: Tuple[TransitVehicleConfig, ...]

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
    input_connector_access_times_path: Optional[str] = Field(default=None)
    input_connector_egress_times_path: Optional[str] = Field(default=None)
    output_stop_usage_path: Optional[str] = Field(default=None)


@dataclass(frozen=True)
class EmmeConfig(ConfigItem):
    """Emme-specific parameters.

    Properties:
        all_day_scenario_id: scenario ID to use for all day
            (initial imported) scenario with all time period data
        project_path: relative path to Emme desktop project (.emp)
        highway_database_path: relative path to highway Emmebank
        active_database_paths: list of relative paths to active mode Emmebanks
        transit_database_path: relative path to transit Emmebank
        num_processors: the number of processors to use in Emme procedures,
            either as an integer, or value MAX, MAX-N. Typically recommend
            using MAX-1 (on desktop systems) or MAX-2 (on servers with many
            logical processors) to leave capacity for background / other tasks.
    """

    all_day_scenario_id: int
    project_path: str
    highway_database_path: str
    active_database_paths: Tuple[str, ...]
    transit_database_path: str
    num_processors: str = Field(regex=r"(?i)^MAX$|^MAX[\s]*-[\s]*[\d]+$|^[\d]+$")


@dataclass(frozen=True)
class Configuration(ConfigItem):
    """Configuration: root of the model configuration"""

    scenario: ScenarioConfig
    run: RunConfig
    time_periods: Tuple[TimePeriodConfig, ...]
    household: HouseholdConfig
    air_passenger: AirPassengerConfig
    internal_external: InternalExternalConfig
    truck: TruckConfig
    active_modes: ActiveModesConfig
    highway: HighwayConfig
    transit: TransitConfig
    emme: EmmeConfig

    @classmethod
    def load_toml(cls, path: Union[str, List[str]]):
        """Load configuration from .toml files(s)

        Normally the config is split into a scenario_config.toml file and a
        model_config.toml file.

        Args:
            path: a valid system path to a TOML format config file or list of paths

        Returns:
            A Configuration object
        """
        if isinstance(path, str):
            path = [path]
        data = _load_toml(path[0])
        for path_item in path[1:]:
            _merge_dicts(data, _load_toml(path_item))
        return cls(**data)

    @classmethod
    @validator("highway")
    def maz_skim_period_exists(cls, value, values):
        """Validate highway.maz_to_maz.skim_period refers to a valid period"""
        if "time_periods" in values:
            time_period_names = set(time.name for time in values["time_periods"])
            assert (
                value.maz_to_maz.skim_period in time_period_names
            ), "maz_to_maz -> skim_period -> name not found in time_periods list"
        return value


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
