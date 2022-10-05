"""Config implementation and schema."""
# pylint: disable=too-many-instance-attributes

import datetime
import pathlib
from abc import ABC
from typing import Dict, List, Optional, Tuple, Union

import toml
from pydantic import Field, NonNegativeFloat, validator
from pydantic.dataclasses import dataclass
from pydantic.error_wrappers import ValidationError
from typing_extensions import Literal


class ConfigItem(ABC):
    """Base class to add partial dict-like interface to tm2py model configuration.

    Allow use of .items() ["X"] and .get("X") .to_dict() from configuration.

    Not to be constructed directly. To be used a mixin for dataclasses
    representing config schema.
    Do not use "get" "to_dict", or "items" for key names.
    """

    def __getitem__(self, key):
        """Get item for config. D[key] -> D[key] if key in D, else raise KeyError."""
        return getattr(self, key)

    def items(self):
        """The sub-config objects in config."""
        return self.__dict__.items()

    def get(self, key, default=None):
        """Return the value for key if key is in the dictionary, else default."""
        return self.__dict__.get(key, default)


@dataclass(frozen=True)
class ScenarioConfig(ConfigItem):
    """Scenario related parameters.

    Properties:
        verify: optional, default False if specified as True components will run
            additional post-process verify step to validate results / outputs
            (not implemented yet)
        maz_landuse_file: relative path to maz_landuse_file used by multiple
            components
        name: scenario name string
        year: model year, must be at least 2005
    """

    maz_landuse_file: pathlib.Path
    name: str
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
    "prepare_network_transit",
    "transit_assign",
    "transit_skim",
    "household",
    "visitor",
    "internal_external",
    "truck",
]
EmptyString = Literal[""]


@dataclass(frozen=True)
class RunConfig(ConfigItem):
    """Model run parameters.

    Note that the components will be executed in the order listed.

    Properties:
        start_iteration: start iteration number, 0 to include initial_components
        end_iteration: final iteration number
        start_component: name of component to start with, will skip components
            list prior to this component
        initial_components: list of components to run as initial (0) iteration, in order
        global_iteration_components: list of component to run at every subsequent
            iteration (max(1, start_iteration) to end_iteration), in order.
        final_components: list of components to run after final iteration, in order
    """

    initial_components: Tuple[ComponentNames, ...]
    global_iteration_components: Tuple[ComponentNames, ...]
    final_components: Tuple[ComponentNames, ...]
    start_iteration: int = Field(ge=0)
    end_iteration: int = Field(gt=0)
    start_component: Optional[Union[ComponentNames, EmptyString]] = Field(default="")

    @validator("end_iteration", allow_reuse=True)
    def end_iteration_gt_start(cls, value, values):
        """Validate end_iteration greater than start_iteration."""
        if values.get("start_iteration"):
            assert (
                value >= values["start_iteration"]
            ), f"'end_iteration' ({value}) must be greater than 'start_iteration'\
                ({values['start_iteration']})"
        return value

    @validator("start_component", allow_reuse=True)
    def start_component_used(cls, value, values):
        """Validate start_component is listed in *_components."""
        if not values.get("start_component") or not value:
            return value

        if "start_iteration" in values:
            if values.get("start_iteration") == 0:
                assert value in values.get(
                    "initial_components", [value]
                ), f"'start_component' ({value}) must be one of the components listed in\
                    initial_components if 'start_iteration = 0'"
            else:
                assert value in values.get(
                    "global_iteration_components", [values]
                ), f"'start_component' ({value}) must be one of the components listed in\
                    global_iteration_components if 'start_iteration > 0'"
        return value


LogLevel = Literal[
    "TRACE", "DEBUG", "DETAIL", "INFO", "STATUS", "WARN", "ERROR", "FATAL"
]


@dataclass(frozen=True)
class LoggingConfig(ConfigItem):
    """Logging parameters. TODO.

    Properties:
        display_level: filter level for messages to show in console, default
            is STATUS
        run_file_path: relative path to high-level log file for the model run,
            default is tm2py_run_[%Y%m%d_%H%M].log
        run_file_level: filter level for messages recorded in the run log,
            default is INFO
        log_file_path: relative path to general log file with more detail
            than the run_file, default is tm2py_detail_[%Y%m%d_%H%M].log
        log_file_level: optional, filter level for messages recorded in the
            standard log, default is DETAIL
        log_on_error_file_path: relative path to use for fallback log message cache
            on error, default is tm2py_error_[%Y%m%d_%H%M].log
        notify_slack: if true notify_slack messages will be sent, default is False
        use_emme_logbook: if True log messages recorded in the standard log file will
            also be recorded in the Emme logbook, default is True
        iter_component_level: tuple of tuples of iteration, component name, log level.
            Used to override log levels (log_file_level) for debugging and recording
            more detail in the log_file_path.
            Example: [ [2, "highway", "TRACE"] ] to record all messages
            during the highway component run at iteration 2.
    """

    display_level: Optional[LogLevel] = Field(default="STATUS")
    run_file_path: Optional[str] = Field(
        default="tm2py_run_{}.log".format(
            datetime.datetime.now().strftime("%Y%m%d_%H%M")
        )
    )
    run_file_level: Optional[LogLevel] = Field(default="INFO")
    log_file_path: Optional[str] = Field(
        default="tm2py_debug_{}.log".format(
            datetime.datetime.now().strftime("%Y%m%d_%H%M")
        )
    )
    log_file_level: Optional[LogLevel] = Field(default="DEBUG")
    log_on_error_file_path: Optional[str] = Field(
        default="tm2py_error_{}.log".format(
            datetime.datetime.now().strftime("%Y%m%d_%H%M")
        )
    )

    notify_slack: Optional[bool] = Field(default=False)
    use_emme_logbook: Optional[bool] = Field(default=True)
    iter_component_level: Optional[
        Tuple[Tuple[int, ComponentNames, LogLevel], ...]
    ] = Field(default=None)


@dataclass(frozen=True)
class TimePeriodConfig(ConfigItem):
    """Time time period entry.

    Properties:
        name: name of the time period, up to four characters
        length_hours: length of the time period in hours
        highway_capacity_factor: factor to use to multiple the per-hour
            capacites in the highway network
        emme_scenario_id: scenario ID to use for Emme per-period
            assignment (highway and transit) scenarios
    """

    name: str = Field(max_length=4)
    length_hours: float = Field(gt=0)
    highway_capacity_factor: float = Field(gt=0)
    emme_scenario_id: int = Field(ge=1)
    description: Optional[str] = Field(default="")


@dataclass(frozen=True)
class TimeSplitConfig(ConfigItem):
    """Split matrix i and j.

    i.e. for time of day splits.
    """

    time_period: str
    production: Optional[NonNegativeFloat] = None
    attraction: Optional[NonNegativeFloat] = None
    od: Optional[NonNegativeFloat] = None

    @validator("production", "attraction", "od")
    def less_than_equal_one(cls, v):
        if v:
            assert v <= 1.0, "Value should be less than or equal to 1"
            return v

    def __post_init__(self):
        if self.od and any([self.production, self.attraction]):
            raise ValueError(
                f"TimeSplitConfig: Must either specifify an od or any of\
            production/attraction - not both.\n{self}"
            )

        if not all([self.production, self.attraction]) and any(
            [self.production, self.attraction]
        ):
            raise ValueError(
                f"TimeSplitConfig: Must have both production AND attraction\
            if one of them is specified."
            )


@dataclass(frozen=True)
class TimeOfDayClassConfig(ConfigItem):
    """Configuration for a class of time of day model."""

    name: str
    time_period_split: List[TimeSplitConfig]


@dataclass(frozen=True)
class TimeOfDayConfig(ConfigItem):
    """Configuration for time of day model."""

    classes: List[TimeOfDayClassConfig]


@dataclass(frozen=True)
class HouseholdConfig(ConfigItem):
    """Household (residents) model parameters."""

    highway_demand_file: pathlib.Path
    transit_demand_file: pathlib.Path


@dataclass(frozen=True)
class AirPassengerDemandAggregationConfig(ConfigItem):
    """Air passenger demand aggregation input parameters.

    Properties:
        name: (src_group_name) name used for the class group in the input columns
            for the trip tables,
        mode: (result_class_name) name used in the output OMX matrix names, note
            that this should match the expected naming convention in the
            HighwayClassDemandConfig name(s)
        access_modes: list of names used for the access modes in the input
            columns for the trip tables
    """

    name: str
    mode: str
    access_modes: Tuple[str, ...]


@dataclass(frozen=True)
class AirPassengerConfig(ConfigItem):
    """Air passenger model parameters.

    Properties

    highway_demand_file: output OMX file
    input_demand_folder: location to find the input demand csvs
    input_demand_filename_tmpl: filename template for input demand. Should have
        {year}, {direction} and {airport} variables and end in '.csv'
    reference_start_year: base start year for input demand tables
        used to calculate the linear interpolation, as well as
        in the file name template {year}_{direction}{airport}.csv
    reference_end_year: end year for input demand tables
        used to calculate the linear interpolation, as well as
        in the file name template {year}_{direction}{airport}.csv
    airport_names: list of one or more airport names / codes as used in
        the input file names
    demand_aggregation: specification of aggregation of by-access mode
        demand to highway class demand
    """

    output_trip_table_directory: pathlib.Path
    outfile_trip_table_tmp: str
    input_demand_folder: pathlib.Path
    input_demand_filename_tmpl: str
    reference_start_year: str
    reference_end_year: str
    airport_names: List[str]
    demand_aggregation: List[AirPassengerDemandAggregationConfig]

    @validator("input_demand_filename_tmpl")
    def valid_input_demand_filename_tmpl(cls, value):
        """Validate skim matrix template has correct {}."""

        assert (
            "{year}" in value
        ), "-> 'output_skim_matrixname_tmpl must have {year}, found {value}."
        assert (
            "{direction}" in value
        ), "-> 'output_skim_matrixname_tmpl must have {direction}, found {value}."
        assert (
            "{airport}" in value
        ), "-> 'output_skim_matrixname_tmpl must have {airport}, found {value}."
        return value


@dataclass(frozen=True)
class MatrixFactorConfig(ConfigItem):
    """Mapping of zone or list of zones to factor value."""

    zone_index: Optional[Union[int, List[int]]]
    factor: Optional[float] = Field(default=None)
    i_factor: Optional[float] = Field(default=None)
    j_factor: Optional[float] = Field(default=None)
    as_growth_rate: Optional[bool] = Field(default=False)

    @validator("zone_index", allow_reuse=True)
    def valid_zone_index(value):
        """Validate zone index and turn to list if isn't one."""
        if isinstance(value, str):
            value = int(value)
        if isinstance(value, int):
            value = [value]
        assert all([x >= 0 for x in value]), "Zone_index must be greater or equal to 0"
        return value

    @validator("factor", allow_reuse=True)
    def valid_factor(value, values):
        assert (
            "i_factor" not in values.keys()
        ), "Found both `factor` and\
            `i_factor` in MatrixFactorConfig. Should be one or the other."

        assert (
            "j_factor" not in values.keys()
        ), "Found both `factor` and\
            `j_factor` in MatrixFactorConfig. Should be one or the other."
        return value


@dataclass(frozen=True)
class CoefficientConfig(ConfigItem):
    """Coefficient and properties to be used in utility or regression."""

    property: str
    coeff: Optional[float] = Field(default=None)


@dataclass(frozen=True)
class ChoiceClassConfig(ConfigItem):
    """Choice class parameters.

    Properties:
        property_to_skim_toll: Maps a property in the utility equation with a list of skim
            properties. If more than one skim property is listed, they will be summed together
            (e.g. cost if the sum of bridge toll and value toll). This defaults to a value in the
            code.
        property_to_skim_notoll: Maps a property in the utility equation with a list of skim
            properties for no toll choice.If more than one skim property is listed, they will
            be summed together  (e.g. cost if the sum of bridge toll and value toll). This
            defaults to a value in the code.
        property_factors: This will scale the property for this class. e.g. a shared ride cost
            could be applied a factor assuming that the cost is shared among individuals.

    The end value in the utility equation for class c and property p is:

       utility[p].coeff *
       classes[c].property_factor[p] *
       sum(skim(classes[c].skim_mode,skim_p) for skim_p in property_to_skim[p])
    """

    name: str
    skim_mode: Optional[str] = Field(default="da")
    veh_group_name: Optional[str] = Field(default="")
    property_factors: Optional[List[CoefficientConfig]] = Field(default=None)


@dataclass(frozen=True)
class TollChoiceConfig(ConfigItem):
    """Toll choice parameters.

    Properties:
        property_to_skim_toll: Maps a property in the utility equation with a list of skim
            properties. If more than one skim property is listed, they will be summed together
            (e.g. cost if the sum of bridge toll and value toll). This defaults to a value in the
            code.
        property_to_skim_notoll: Maps a property in the utility equation with a list of skim
            properties for no toll choice.If more than one skim property is listed, they will
            be summed together  (e.g. cost if the sum of bridge toll and value toll). This
            defaults to a value in the code.
    """

    classes: List[ChoiceClassConfig]
    value_of_time: float
    operating_cost_per_mile: float
    property_to_skim_toll: Optional[Dict[str, List[str]]] = Field(default_factory=dict)
    property_to_skim_notoll: Optional[Dict[str, List[str]]] = Field(
        default_factory=dict
    )
    utility: Optional[List[CoefficientConfig]] = Field(default_factory=dict)


@dataclass(frozen=True)
class DemandGrowth(ConfigItem):
    input_demand_file: pathlib.Path
    input_demand_matrixname_tmpl: str
    reference_year: int
    annual_growth_rate: List[MatrixFactorConfig]
    special_gateway_adjust: Optional[List[MatrixFactorConfig]] = Field(
        default_factory=list
    )


@dataclass(frozen=True)
class InternalExternalConfig(ConfigItem):
    """Internal <-> External model parameters."""

    output_trip_table_directory: pathlib.Path
    outfile_trip_table_tmp: str
    modes: List[str]
    demand: DemandGrowth
    time_of_day: TimeOfDayConfig
    toll_choice: TollChoiceConfig


@dataclass(frozen=True)
class TripGenerationFormulaConfig(ConfigItem):
    """TripProductionConfig.

    Trip productions or attractions for a zone are the constant plus the sum of the rates * values
    in land use file for that zone.
    """

    land_use_rates: List[CoefficientConfig]
    constant: Optional[float] = Field(default=0.0)
    multiplier: Optional[float] = Field(default=1.0)


@dataclass(frozen=True)
class TripGenerationClassConfig(ConfigItem):
    """Trip Generation parameters."""

    name: str
    purpose: Optional[str] = Field(default=None)
    production_formula: Optional[TripGenerationFormulaConfig] = Field(default=None)
    attraction_formula: Optional[TripGenerationFormulaConfig] = Field(default=None)
    balance_to: Optional[str] = Field(default="production")


@dataclass(frozen=True)
class TripGenerationConfig(ConfigItem):
    """Trip Generation parameters."""

    classes: List[TripGenerationClassConfig]


@dataclass(frozen=True)
class TripDistributionClassConfig(ConfigItem):
    """Trip Distribution parameters.

    Properties:
        name: name of class to apply to
        impedance_name: refers to an impedance (skim) matrix to use - often a blended skim.
        use_k_factors: boolean on if to use k-factors
    """

    name: str
    impedance: str
    use_k_factors: bool


@dataclass(frozen=True)
class TruckClassConfig(ConfigItem):
    """Truck class parameters."""

    name: str
    description: Optional[str] = ""


@dataclass(frozen=True)
class ImpedanceConfig(ConfigItem):
    """Blended skims used for accessibility/friction calculations.

    Properties:I
        name: name to store it as, referred to in TripDistribution config
        skim_mode: name of the mode to use for the blended skim
        time_blend: blend of time periods to use; mapped to the factors (which should sum to 1)
    """

    name: str
    skim_mode: str
    time_blend: Dict[str, float]

    @validator("time_blend", allow_reuse=True)
    def sums_to_one(value):
        """Validate highway.maz_to_maz.skim_period refers to a valid period."""
        assert sum(value.values()) - 1 < 0.0001, "blend factors must sum to 1"
        return value


@dataclass(frozen=True)
class TripDistributionConfig(ConfigItem):
    """Trip Distribution parameters."""

    classes: List[TripDistributionClassConfig]
    max_balance_iterations: int
    max_balance_relative_error: float
    friction_factors_file: pathlib.Path
    k_factors_file: Optional[pathlib.Path] = None


@dataclass(frozen=True)
class TruckConfig(ConfigItem):
    """Truck model parameters."""

    classes: List[TruckClassConfig]
    impedances: List[ImpedanceConfig]
    trip_gen: TripGenerationConfig
    trip_dist: TripDistributionConfig
    time_of_day: TimeOfDayConfig
    toll_choice: TollChoiceConfig
    output_trip_table_directory: pathlib.Path
    outfile_trip_table_tmp: str

    """
    @validator("classes")
    def class_consistency(cls, v, values):
        # TODO Can't get to work righ tnow
        _class_names = [c.name for c in v]
        _gen_classes = [c.name for c in values["trip_gen"]]
        _dist_classes = [c.name for c in values["trip_dist"]]
        _time_classes = [c.name for c in values["time_split"]]
        _toll_classes = [c.name for c in values["toll_choice"]]

        assert (
            _class_names == _gen_classes
        ), "truck.classes ({_class_names}) doesn't equal\
            class names in truck.trip_gen ({_gen_classes})."
        assert (
            _class_names == _dist_classes
        ), "truck.classes ({_class_names}) doesn't  equal\
            class names in truck.trip_dist ({_dist_classes})."
        assert (
            _class_names == _time_classes
        ), "truck.classes ({_class_names}) doesn't  equal\
            class names in truck.time_split ({_time_classes})."
        assert (
            _class_names == _toll_classes
        ), "truck.classes ({_class_names}) doesn't equal\
            class names in truck.toll_choice ({_toll_classes})."

        return v
    """


@dataclass(frozen=True)
class ActiveModeShortestPathSkimConfig(ConfigItem):
    """Active mode skim entry."""

    mode: str
    roots: str
    leaves: str
    output: str
    max_dist_miles: float = None


@dataclass(frozen=True)
class ActiveModesConfig(ConfigItem):
    """Active Mode skim parameters."""

    emme_scenario_id: int
    shortest_path_skims: Tuple[ActiveModeShortestPathSkimConfig, ...]


@dataclass(frozen=True)
class HighwayCapClassConfig(ConfigItem):
    """Highway link capacity and speed ('capclass') index entry.

    Properties:
        capclass: cross index for link @capclass lookup
        capacity: value for link capacity, PCE / hour
        free_flow_speed: value for link free flow speed, miles / hour
        critical_speed: value for critical speed (Ja) used in Akcelik
            type functions
    """

    capclass: int = Field(ge=0)
    capacity: float = Field(ge=0)
    free_flow_speed: float = Field(ge=0)
    critical_speed: float = Field(ge=0)


@dataclass(frozen=True)
class ClassDemandConfig(ConfigItem):
    """Input source for demand for highway or transit assignment class.

    Used to specify where to find related demand file for this
    highway or transit class.

    Properties:
        source: reference name of the component section for the
                source "highway_demand_file" (for a highway class)
                or "transit_demand_file" (for a transit class), one of:
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
            Should be unique in list of :es, unless multiple classes
            have identical excluded_links specification. Cannot be the
            same as used for highway.maz_to_maz.mode_code.
        value_of_time: value of time for this class in $ / hr
        operating_cost_per_mile: vehicle operating cost in cents / mile
        demand: list of OMX file and matrix keyname references,
            see ClassDemandConfig
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
    veh_group_name: str = Field(min_length=1, max_length=10)
    description: Optional[str] = Field(default="")
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
    demand: Tuple[ClassDemandConfig, ...] = Field()


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
            resulting skim matrices. Cross-referenced in list of highway.classes[],
            valid keywords for:
                excluded_links: "is_toll_{vehicle}"
                tolls: "@bridgetoll_{vehicle}", "@valuetoll_{vehicle}"
                skims: "bridgetoll_{vehicle}", "valuetoll_{vehicle}"
    """

    file_path: pathlib.Path = Field()
    tollbooth_start_index: int = Field(gt=1)
    src_vehicle_group_names: Tuple[str, ...] = Field()
    dst_vehicle_group_names: Tuple[str, ...] = Field()

    @validator("dst_vehicle_group_names", always=True)
    def dst_vehicle_group_names_length(cls, value, values):
        """Validate dst_vehicle_group_names has same length as src_vehicle_group_names."""
        if "src_vehicle_group_names" in values:
            assert len(value) == len(
                values["src_vehicle_group_names"]
            ), "dst_vehicle_group_names must be same length as src_vehicle_group_names"
            assert all(
                [len(v) <= 4 for v in value]
            ), "dst_vehicle_group_names must be 4 characters or less"
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
    """Grouping of counties for assignment and demand files.

    Properties:
        number: id number for this group, must be unique
        counties: list of one or more county names
    """

    number: int = Field()
    counties: Tuple[COUNTY_NAMES, ...] = Field()


@dataclass(frozen=True)
class HighwayMazToMazConfig(ConfigItem):
    """Highway MAZ to MAZ shortest path assignment and skim parameters.

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
    demand_file: pathlib.Path = Field()
    demand_county_groups: Tuple[DemandCountyGroupConfig, ...] = Field()
    skim_period: str = Field()
    output_skim_file: pathlib.Path = Field()

    @validator("demand_county_groups")
    def unique_group_numbers(cls, value):
        """Validate list of demand_county_groups has unique .number values."""
        group_ids = [group.number for group in value]
        assert len(group_ids) == len(set(group_ids)), "-> number value must be unique"
        return value


@dataclass(frozen=True)
class HighwayConfig(ConfigItem):
    """Highway assignment and skims parameters.

    Properties:
        generic_highway_mode_code: single character unique mode ID for entire
            highway network (no excluded_links)
        relative_gap: target relative gap stopping criteria
        max_iterations: maximum iterations stopping criteria
        area_type_buffer_dist_miles: used to in calculation to categorize link @areatype
            The area type is determined based on the average density of nearby
            (within this buffer distance) MAZs, using (pop+jobs*2.5)/acres
        output_skim_path: relative path template from run dir for OMX output skims
        output_skim_filename_tmpl: template for OMX filename for a time period. Must include
            {time_period} in the string and end in '.omx'.
        output_skim_matrixname_tmpl: template for matrix names within OMX output skims.
            Should include {time_period}, {mode}, and {property}
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
    output_skim_path: pathlib.Path = Field()
    output_skim_filename_tmpl: str = Field()
    output_skim_matrixname_tmpl: str = Field()
    tolls: HighwayTollsConfig = Field()
    maz_to_maz: HighwayMazToMazConfig = Field()
    classes: Tuple[HighwayClassConfig, ...] = Field()
    capclass_lookup: Tuple[HighwayCapClassConfig, ...] = Field()

    @validator("output_skim_filename_tmpl")
    def valid_skim_template(value):
        """Validate skim template has correct {} and extension."""
        assert (
            "{time_period" in value
        ), f"-> output_skim_filename_tmpl must have {{time_period}}', found {value}."
        assert (
            value[-4:].lower() == ".omx"
        ), f"-> 'output_skim_filename_tmpl must end in '.omx', found {value[-4:].lower() }"
        return value

    @validator("output_skim_matrixname_tmpl")
    def valid_skim_matrix_name_template(value):
        """Validate skim matrix template has correct {}."""
        assert (
            "{time_period" in value
        ), "-> 'output_skim_matrixname_tmpl must have {time_period}, found {value}."
        assert (
            "{property" in value
        ), "-> 'output_skim_matrixname_tmpl must have {property}, found {value}."
        assert (
            "{mode" in value
        ), "-> 'output_skim_matrixname_tmpl must have {mode}, found {value}."
        return value

    @validator("capclass_lookup")
    def unique_capclass_numbers(cls, value):
        """Validate list of capclass_lookup has unique .capclass values."""
        capclass_ids = [i.capclass for i in value]
        error_msg = "-> capclass value must be unique in list"
        assert len(capclass_ids) == len(set(capclass_ids)), error_msg
        return value

    @validator("classes", pre=True)
    def unique_class_names(cls, value):
        """Validate list of classes has unique .name values."""
        class_names = [highway_class["name"] for highway_class in value]
        error_msg = "-> name value must be unique in list"
        assert len(class_names) == len(set(class_names)), error_msg
        return value

    @validator("classes")
    def validate_class_mode_excluded_links(cls, value, values):
        """Validate list of classes has unique .mode_code or .excluded_links match."""
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

    @validator("classes")
    def validate_class_keyword_lists(cls, value, values):
        """Validate classes .skims, .toll, and .excluded_links values."""
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
    """Transit mode definition (see also mode in the Emme API)."""

    type: Literal["WALK", "ACCESS", "EGRESS", "LOCAL", "PREMIUM"]
    assign_type: Literal["TRANSIT", "AUX_TRANSIT"]
    mode_id: str = Field(min_length=1, max_length=1)
    name: str = Field(max_length=10)
    description: Optional[str] = ""
    in_vehicle_perception_factor: Optional[float] = Field(default=None, ge=0)
    speed_miles_per_hour: Optional[float] = Field(default=None, gt=0)
    eawt_factor: Optional[float] = Field(default=1)

    @validator("in_vehicle_perception_factor", always=True)
    def in_vehicle_perception_factor_valid(cls, value, values):
        """Validate in_vehicle_perception_factor exists if assign_type is TRANSIT."""
        if "assign_type" in values and values["assign_type"] == "TRANSIT":
            assert value is not None, "must be specified when assign_type==TRANSIT"
        return value

    @validator("speed_miles_per_hour", always=True)
    def speed_miles_per_hour_valid(cls, value, values):
        """Validate speed_miles_per_hour exists if assign_type is AUX_TRANSIT."""
        if "assign_type" in values and values["assign_type"] == "AUX_TRANSIT":
            assert value is not None, "must be specified when assign_type==AUX_TRANSIT"
        return value

    @classmethod
    @validator("speed_miles_per_hour")
    def mode_id_valid(cls, value):
        """Validate mode_id."""
        assert len(value) == 1, "mode_id must be one character"
        return value


@dataclass(frozen=True)
class TransitVehicleConfig(ConfigItem):
    """Transit vehicle definition (see also transit vehicle in the Emme API)."""

    vehicle_id: int
    mode: str
    name: str
    auto_equivalent: Optional[float] = Field(default=0, ge=0)
    seated_capacity: Optional[int] = Field(default=None, ge=0)
    total_capacity: Optional[int] = Field(default=None, ge=0)


@dataclass(frozen=True)
class TransitClassConfig(ConfigItem):
    """Transit demand class definition."""

    skim_set_id: str
    name: str
    description: str
    mode_types: Tuple[str, ...]
    demand: Tuple[ClassDemandConfig, ...]
    required_mode_combo: Optional[Tuple[str, ...]] = Field(default=None)


@dataclass(frozen=True)
class AssignmentStoppingCriteriaConfig(ConfigItem):
    "Assignment stop configuration parameters."
    max_iterations: int
    relative_difference: float
    percent_segments_over_capacity: float


@dataclass(frozen=True)
class CcrWeightsConfig(ConfigItem):
    "Weights for CCR Configuration."
    min_seat: float = Field(default=1.0)
    max_seat: float = Field(default=1.4)
    power_seat: float = Field(default=2.2)
    min_stand: float = Field(default=1.4)
    max_stand: float = Field(default=1.6)
    power_stand: float = Field(default=3.4)


@dataclass(frozen=True)
class EawtWeightsConfig(ConfigItem):
    "Weights for calculating extra added wait time Configuration."
    constant: float = Field(default=0.259625)
    weight_inverse_headway: float = Field(default=1.612019)
    vcr: float = Field(default=0.005274)
    exit_proportion: float = Field(default=0.591765)
    default_eawt_factor: float = Field(default=1)


@dataclass(frozen=True)
class TransitConfig(ConfigItem):
    """Transit assignment parameters."""

    modes: Tuple[TransitModeConfig, ...]
    vehicles: Tuple[TransitVehicleConfig, ...]
    classes: Tuple[TransitClassConfig, ...]
    value_of_time: float
    effective_headway_source: str
    initial_wait_perception_factor: float
    transfer_wait_perception_factor: float
    walk_perception_factor: float
    initial_boarding_penalty: float
    transfer_boarding_penalty: float
    max_transfers: int
    output_skim_path: pathlib.Path
    fares_path: pathlib.Path
    fare_matrix_path: pathlib.Path
    fare_max_transfer_distance_miles: float
    use_fares: bool
    use_ccr: bool
    ccr_stop_criteria: Optional[AssignmentStoppingCriteriaConfig]
    ccr_weights: CcrWeightsConfig
    eawt_weights: EawtWeightsConfig
    override_connector_times: bool
    apply_msa_demand: bool
    max_ccr_iterations: float = None
    split_connectors_to_prevent_walk: bool = True
    input_connector_access_times_path: Optional[str] = Field(default=None)
    input_connector_egress_times_path: Optional[str] = Field(default=None)
    output_stop_usage_path: Optional[str] = Field(default=None)
    output_transit_boardings_path: Optional[str] = Field(default=None)


@dataclass(frozen=True)
class EmmeConfig(ConfigItem):
    """Emme-specific parameters.

    Properties:
        all_day_scenario_id: scenario ID to use for all day
            (initial imported) scenario with all time period data
        project_path: relative path from run_dir to Emme desktop project (.emp)
        highway_database_path: relative path to highway Emmebank
        active_north_database_path:  relative paths to active mode Emmebank for north bay
        active_south_database_path:  relative paths to active mode Emmebank for south bay
        transit_database_path: relative path to transit Emmebank
        num_processors: the number of processors to use in Emme procedures,
            either as an integer, or value MAX, MAX-N. Typically recommend
            using MAX-1 (on desktop systems) or MAX-2 (on servers with many
            logical processors) to leave capacity for background / other tasks.
    """

    all_day_scenario_id: int
    project_path: pathlib.Path
    highway_database_path: pathlib.Path
    active_north_database_path: pathlib.Path
    active_south_database_path: pathlib.Path
    transit_database_path: pathlib.Path
    num_processors: str = Field(regex=r"^MAX$|^MAX-\d+$|^\d+$")


@dataclass(frozen=True)
class Configuration(ConfigItem):
    """Configuration: root of the model configuration."""

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
    logging: Optional[LoggingConfig] = Field(default_factory=LoggingConfig)

    @classmethod
    def load_toml(
        cls,
        toml_path: Union[List[Union[str, pathlib.Path]], str, pathlib.Path],
    ) -> "Configuration":
        """Load configuration from .toml files(s).

        Normally the config is split into a scenario_config.toml file and a
        model_config.toml file.

        Args:
            toml_path: a valid system path string or Path object to a TOML format config file or
                list of paths of path objects to a set of TOML files.

        Returns:
            A Configuration object
        """
        if not isinstance(toml_path, List):
            toml_path = [toml_path]
        toml_path = list(map(pathlib.Path, toml_path))

        data = _load_toml(toml_path[0])
        for path_item in toml_path[1:]:
            _merge_dicts(data, _load_toml(path_item))
        return cls(**data)

    @validator("highway")
    def maz_skim_period_exists(cls, value, values):
        """Validate highway.maz_to_maz.skim_period refers to a valid period."""
        if "time_periods" in values:
            time_period_names = set(time.name for time in values["time_periods"])
            assert (
                value.maz_to_maz.skim_period in time_period_names
            ), "maz_to_maz -> skim_period -> name not found in time_periods list"
        return value


def _load_toml(path: str) -> dict:
    """Load config from toml file at path."""
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
