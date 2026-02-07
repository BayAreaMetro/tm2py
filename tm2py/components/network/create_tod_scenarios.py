"""
"""

import os
from collections import defaultdict as _defaultdict
from contextlib import contextmanager as _context
from typing import TYPE_CHECKING, Any, Dict, Tuple, Union

from tm2py.components.component import Component
from tm2py.logger import LogStartEnd
from tm2py.tools import SpatialGridIndex

if TYPE_CHECKING:
    from tm2py.controller import RunController

_crs_wkt = """PROJCS["NAD83(HARN) / California zone 6 (ftUS)",GEOGCS["NAD83(HARN)",
DATUM["NAD83_High_Accuracy_Reference_Network",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],
TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6152"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",
0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4152"]],PROJECTION["Lambert_Conformal_Conic_2SP"],
PARAMETER["standard_parallel_1",33.88333333333333],PARAMETER["standard_parallel_2",32.78333333333333],
PARAMETER["latitude_of_origin",32.16666666666666],PARAMETER["central_meridian",-116.25],PARAMETER["false_easting",
6561666.667],PARAMETER["false_northing",1640416.667],UNIT["US survey foot",0.3048006096012192,AUTHORITY["EPSG",
"9003"]],AXIS["X",EAST],AXIS["Y",NORTH],AUTHORITY["EPSG","2875"]] """


class CreateTODScenarios(Component):
    """Highway assignment and skims"""

    def __init__(self, controller: "RunController"):
        """Highway assignment and skims.

        Args:
            controller: parent Controller object
        """
        super().__init__(controller)
        self._emme_manager = None
        self._ref_auto_network = None

    def validate_inputs(self):
        """Validate the inputs."""
        # TODO

    def run(self):
        # project_path = self.get_abs_path(self.controller.config.emme.project_path)
        # self._emme_manager = self.controller.emme_manager
        # emme_app = self._emme_manager.project(project_path)
        # self._emme_manager.init_modeller(emme_app)
        with self._setup():
            self._create_highway_scenarios()
            
            # Skip transit scenarios if highway_only flag is set or no transit component in run
            skip_transit = False
            if hasattr(self.controller.config.emme, 'highway_only'):
                skip_transit = self.controller.config.emme.highway_only
            elif hasattr(self.controller.config, 'run'):
                # Check if transit component is in the component lists
                all_components = (
                    list(getattr(self.controller.config.run, 'initial_components', [])) +
                    list(getattr(self.controller.config.run, 'global_iteration_components', [])) +
                    list(getattr(self.controller.config.run, 'final_components', []))
                )
                has_transit = 'transit' in all_components
                skip_transit = not has_transit
            
            if skip_transit:
                self.controller.debug("Skipping _create_transit_scenarios() - highway-only mode")
                self.controller.logger.log("Skipping transit scenario creation (highway-only mode)", level="INFO")
            else:
                self._create_transit_scenarios()

    @_context
    def _setup(self):
        self._ref_auto_network = None
        try:
            yield
        finally:
            self._ref_auto_network = None

    def _project_coordinates(self, ref_scenario):
        modeller = self.controller.emme_manager.modeller
        project_coord = modeller.tool(
            "inro.emme.data.network.base.project_network_coordinates"
        )

        project_path = self.get_abs_path(self.controller.config.emme.project_path)
        project_root = os.path.dirname(project_path)
        emme_app = self.controller.emme_manager.project(project_path)
        src_prj_file = emme_app.project.spatial_reference_file
        if not src_prj_file:
            raise Exception(
                "Emme network coordinate reference system is not specified, unable to project coordinates for "
                "area type calculation. Set correct Spatial Reference in Emme Project settings -> GIS."
            )
        with open(src_prj_file, "r") as src_prj:
            current_wkt = src_prj.read()
        if current_wkt != _crs_wkt:
            dst_prj_file = os.path.join(
                project_root, "Media", "NAD83(HARN) California zone 6 (ftUS).prj"
            )
            with open(dst_prj_file, "w") as dst_prj:
                dst_prj.write(_crs_wkt)
            project_coord(
                from_scenario=ref_scenario,
                from_proj_file=src_prj_file,
                to_proj_file=dst_prj_file,
                overwrite=True,
            )
            emme_app.project.spatial_reference.file_path = dst_prj_file
            emme_app.project.save()

    @LogStartEnd("Create highway time of day scenarios.")
    def _create_highway_scenarios(self):
        emmebank = self.controller.emme_manager.highway_emmebank.emmebank
        ref_scenario = emmebank.scenario(
            self.controller.config.emme.all_day_scenario_id
        )
        self._ref_auto_network = ref_scenario.get_network()
        n_time_periods = len(self.controller.config.time_periods)
        self.controller.emme_manager.highway_emmebank.change_dimensions(
            {
                "scenarios": 1 + n_time_periods,
                "full_matrices": 9999,
                "extra_attribute_values": 100000000,
            }
        )
        # create VDFs & set cross-reference function parameters
        emmebank.extra_function_parameters.el1 = "@free_flow_time"
        emmebank.extra_function_parameters.el2 = "@capacity"
        emmebank.extra_function_parameters.el3 = "@ja"
        emmebank.extra_function_parameters.el4 = "@static_rel"
        # get() and put() did not work for los reliability
        # remove them from the reliability tmplt
        reliability_tmplt = (
            "* (1 + el4 + "
            "( {factor[LOS_C]} * ( ((volau + volad)/el2).min.1.5 - {threshold[LOS_C]} + 0.01 ) ) * (((volau + volad)/el2) .gt. {threshold[LOS_C]})"
            "+ ( {factor[LOS_D]} * ( ((volau + volad)/el2).min.1.5 - {threshold[LOS_D]} + 0.01 )  ) * (((volau + volad)/el2) .gt. {threshold[LOS_D]})"
            "+ ( {factor[LOS_E]} * ( ((volau + volad)/el2).min.1.5 - {threshold[LOS_E]} + 0.01 )  ) * (((volau + volad)/el2) .gt. {threshold[LOS_E]})"
            "+ ( {factor[LOS_FL]} * ( ((volau + volad)/el2).min.1.5 - {threshold[LOS_FL]} + 0.01 )  ) * (((volau + volad)/el2) .gt. {threshold[LOS_FL]})"
            "+ ( {factor[LOS_FH]} * ( ((volau + volad)/el2).min.1.5 - {threshold[LOS_FH]} + 0.01 )  ) * (((volau + volad)/el2) .gt. {threshold[LOS_FH]})"
            ")"
        )
        parameters = {
            "freeway": {
                "factor": {
                    "LOS_C": 0.2429,
                    "LOS_D": 0.1705,
                    "LOS_E": -0.2278,
                    "LOS_FL": -0.1983,
                    "LOS_FH": 1.022,
                },
                "threshold": {
                    "LOS_C": 0.7,
                    "LOS_D": 0.8,
                    "LOS_E": 0.9,
                    "LOS_FL": 1.0,
                    "LOS_FH": 1.2,
                },
            },
            "road": {  # for arterials, ramps, collectors, local roads, etc.
                "factor": {
                    "LOS_C": 0.1561,
                    "LOS_D": 0.0,
                    "LOS_E": 0.0,
                    "LOS_FL": -0.449,
                    "LOS_FH": 0.0,
                },
                "threshold": {
                    "LOS_C": 0.7,
                    "LOS_D": 0.8,
                    "LOS_E": 0.9,
                    "LOS_FL": 1.0,
                    "LOS_FH": 1.2,
                },
            },
        }
        # rewrite bpr_tmplt to use put() and get() for nested functions
        # keeping the original for reference
        # bpr_tmplt = "el1 * (1 + 0.20 * ((volau + volad)/el2/0.75)^6)"
        bpr_tmplt = "el1 * (1 + 0.20 * (put((volau + volad)/el2)/0.75) ** 6)"

        fixed_tmplt = "el1"

        # rewrite akcelik_tmplt to use put() and get() for nested functions
        # keeping the original for reference
        # akcelik_tmplt = (
        #     "(el1 + 60 * (0.25 *((volau + volad)/el2 - 1 + "
        #     "(((volau + volad)/el2 - 1)^2 + el3 * (volau + volad)/el2)^0.5)))"
        # )
        akcelik_tmplt = (
            "(el1 + 60 * (0.25 * (put((volau + volad)/el2) - 1 + "
            "((get(1) - 1) ** 2 + el3 * get(1)) ** 0.5)))"
        )

        for f_id in ["fd1", "fd2"]:
            if emmebank.function(f_id):
                emmebank.delete_function(f_id)
            emmebank.create_function(
                f_id, bpr_tmplt + reliability_tmplt.format(**parameters["freeway"])
            )
        for f_id in [
            "fd3",
            "fd4",
            "fd5",
            "fd6",
            "fd7",
            "fd9",
            "fd10",
            "fd11",
            "fd12",
            "fd13",
            "fd14",
            "fd99",
        ]:
            if emmebank.function(f_id):
                emmebank.delete_function(f_id)
            emmebank.create_function(
                f_id, akcelik_tmplt + reliability_tmplt.format(**parameters["road"])
            )
        if emmebank.function("fd8"):
            emmebank.delete_function("fd8")
        emmebank.create_function("fd8", fixed_tmplt)

        ref_scenario = emmebank.scenario(
            self.controller.config.emme.all_day_scenario_id
        )
        attributes = {
            "LINK": ["@area_type", "@capclass", "@free_flow_speed", "@free_flow_time", "@lanes"]
        }
        for domain, attrs in attributes.items():
            for name in attrs:
                if ref_scenario.extra_attribute(name) is None:
                    ref_scenario.create_extra_attribute(domain, name)

        network = ref_scenario.get_network()
        
        # Copy standard lanes to @lanes if @lanes is empty (for legacy networks)
        # get_attribute_values returns [id_array, value_array], so we need the second element
        self.controller.logger.log("Getting @lanes attribute values...", level="DEBUG")
        try:
            lanes_result = ref_scenario.get_attribute_values("LINK", ["@lanes"])
            self.controller.logger.log(f"lanes_result type: {type(lanes_result)}, len: {len(lanes_result) if hasattr(lanes_result, '__len__') else 'N/A'}", level="DEBUG")
            
            if isinstance(lanes_result, list) and len(lanes_result) > 1:
                lanes_values = lanes_result[1]
                self.controller.logger.log(f"lanes_values type: {type(lanes_values)}, len: {len(lanes_values) if hasattr(lanes_values, '__len__') else 'N/A'}", level="DEBUG")
            else:
                lanes_values = lanes_result
                self.controller.logger.log(f"lanes_result not a list, using directly", level="DEBUG")
            
            # Check if all lane values are 0 (need to copy from standard 'num_lanes' attribute)
            if hasattr(lanes_values, '__iter__'):
                # Check first few values
                sample = list(lanes_values[:10]) if hasattr(lanes_values, '__getitem__') else list(lanes_values)[:10]
                self.controller.logger.log(f"First 10 lane values: {sample}", level="DEBUG")
                all_zero = all(v == 0 for v in lanes_values)
            else:
                all_zero = lanes_values == 0
            
            self.controller.logger.log(f"all_zero = {all_zero}", level="DEBUG")
            
            if all_zero:
                self.controller.logger.log(
                    "Copying standard 'num_lanes' attribute to '@lanes' (legacy network compatibility)",
                    level="INFO"
                )
                # Check if num_lanes exists on first link
                first_link = next(iter(network.links()), None)
                if first_link:
                    self.controller.logger.log(f"First link attributes: {list(first_link.network.attributes('LINK'))[:20]}", level="DEBUG")
                    self.controller.logger.log(f"First link num_lanes = {first_link.num_lanes}", level="DEBUG")
                
                for link in network.links():
                    link["@lanes"] = link.num_lanes
                self.controller.logger.log("Finished copying num_lanes to @lanes", level="DEBUG")
        except Exception as e:
            self.controller.logger.log(f"ERROR in lanes copying: {type(e).__name__}: {e}", level="ERROR")
            import traceback
            self.controller.logger.log(f"Traceback: {traceback.format_exc()}", level="ERROR")
            raise
        
        self._set_area_type(network)
        self._set_capclass(network)
        self._set_speed(network)
        ref_scenario.publish_network(network)
        self._ref_auto_network = network

        self._prepare_scenarios_and_attributes(emmebank)

    @LogStartEnd("Create transit time of day scenarios.")
    def _create_transit_scenarios(self):
        with self.logger.log_start_end("prepare base scenario"):
            emmebank = self.controller.emme_manager.transit_emmebank.emmebank
            n_time_periods = len(self.controller.config.time_periods)
            required_dims = {
                "full_matrices": 9999,
                "scenarios": 1 + n_time_periods,
                "regular_nodes": 650000,
                "links": 1900000,
                "transit_vehicles": 600,  # pnr vechiles
                "transit_segments": 1800000,
                "extra_attribute_values": 200000000,
            }
            self.controller.emme_manager.transit_emmebank.change_dimensions(
                required_dims
            )
            for ident in ["ft1", "ft2", "ft3"]:
                if emmebank.function(ident):
                    emmebank.delete_function(ident)
            # for zero-cost links
            emmebank.create_function("ft1", "0")
            # segment travel time pre-calculated and stored in data1 (copied from @trantime_seg)
            emmebank.create_function("ft2", "us1")

            ref_scenario = emmebank.scenario(
                self.controller.config.emme.all_day_scenario_id
            )
            attributes = {
                "LINK": [
                    "@trantime",
                    "@area_type",
                    "@capclass",
                    "@free_flow_speed",
                    "@free_flow_time",
                    "@drive_toll",
                ],
                "TRANSIT_LINE": [
                    "@invehicle_factor",
                    "@iboard_penalty",
                    "@xboard_penalty",
                    "@orig_hdw",
                ],
                "NODE": ["@hdw_fraction", "@wait_pfactor", "@xboard_nodepen"],
            }
            for domain, attrs in attributes.items():
                for name in attrs:
                    if ref_scenario.extra_attribute(name) is None:
                        ref_scenario.create_extra_attribute(domain, name)
            network = ref_scenario.get_network()
            # auto_network = self._ref_auto_network
            # # copy link attributes from auto network to transit network
            # link_lookup = {}
            # for link in auto_network.links():
            #     link_lookup[link["#link_id"]] = link
            # for link in network.links():
            #     auto_link = link_lookup.get(link["#link_id"])
            #     if not auto_link:
            #         continue
            #     for attr in [
            #         "@area_type",
            #         "@capclass",
            #         "@free_flow_speed",
            #         "@free_flow_time",
            #     ]:
            #         link[attr] = auto_link[attr]

            mode_table = self.controller.config.transit.modes
            in_vehicle_factors = {}
            initial_boarding_penalty = {}
            transfer_boarding_penalty = {}
            headway_fraction = {}
            transfer_wait_perception_factor = {}

            default_in_vehicle_factor = self.controller.config.transit.get(
                "in_vehicle_perception_factor", 1.0
            )
            default_initial_boarding_penalty = self.controller.config.transit.get(
                "initial_boarding_penalty", 10
            )
            default_transfer_boarding_penalty = self.controller.config.transit.get(
                "transfer_boarding_penalty", 10
            )
            default_headway_fraction = self.controller.config.transit.get(
                "headway_fraction", 0.5
            )
            default_transfer_wait_perception_factor = (
                self.controller.config.transit.get("transfer_wait_perception_factor", 1)
            )
            walk_perception_factor = self.controller.config.transit.get(
                "walk_perception_factor", 2
            )
            walk_perception_factor_cbd = self.controller.config.transit.get(
                "walk_perception_factor_cbd", 1
            )
            drive_perception_factor = self.controller.config.transit.get(
                "drive_perception_factor", 2
            )
            # walk_modes = set()
            # access_modes = set()
            # egress_modes = set()
            # local_modes = set()
            # premium_modes = set()
            for mode_data in mode_table:
                mode = network.mode(mode_data["mode_id"])
                if mode is None:
                    mode = network.create_mode(
                        mode_data["assign_type"], mode_data["mode_id"]
                    )
                elif mode.type != mode_data["assign_type"]:
                    raise Exception(
                        f"mode {mode_data['id']} already exists with type {mode.type} instead of {mode_data['assign_type']}"
                    )
                mode.description = mode_data["name"]
                if mode_data["assign_type"] == "AUX_TRANSIT":
                    if mode_data["type"] == "DRIVE":
                        mode.speed = "ul1*%s" % drive_perception_factor
                    else:
                        mode.speed = mode_data["speed_or_time_factor"]
                # if mode_data["assign_type"] == "AUX_TRANSIT":
                #     mode.speed = mode_data["speed_miles_per_hour"]
                # if mode_data["type"] == "WALK":
                #     walk_modes.add(mode.id)
                # elif mode_data["type"] == "ACCESS":
                #     access_modes.add(mode.id)
                # elif mode_data["type"] == "EGRESS":
                #     egress_modes.add(mode.id)
                # elif mode_data["type"] == "LOCAL":
                #     local_modes.add(mode.id)
                # elif mode_data["type"] == "PREMIUM":
                #     premium_modes.add(mode.id)
                in_vehicle_factors[mode.id] = mode_data.get(
                    "in_vehicle_perception_factor", default_in_vehicle_factor
                )
                initial_boarding_penalty[mode.id] = mode_data.get(
                    "initial_boarding_penalty", default_initial_boarding_penalty
                )
                transfer_boarding_penalty[mode.id] = mode_data.get(
                    "transfer_boarding_penalty", default_transfer_boarding_penalty
                )
                headway_fraction[mode.id] = mode_data.get(
                    "headway_fraction", default_headway_fraction
                )
                transfer_wait_perception_factor[mode.id] = mode_data.get(
                    "transfer_wait_perception_factor",
                    default_transfer_wait_perception_factor,
                )

            # create vehicles
            # vehicle_table = self.controller.config.transit.vehicles
            # for veh_data in vehicle_table:
            #     vehicle = network.transit_vehicle(veh_data["vehicle_id"])
            #     if vehicle is None:
            #         vehicle = network.create_transit_vehicle(
            #             veh_data["vehicle_id"], veh_data["mode"]
            #         )
            #     elif vehicle.mode.id != veh_data["mode"]:
            #         raise Exception(
            #             f"vehicle {veh_data['vehicle_id']} already exists with mode {vehicle.mode.id} instead of {veh_data['mode']}"
            #         )
            #     vehicle.auto_equivalent = veh_data["auto_equivalent"]
            #     vehicle.seated_capacity = veh_data["seated_capacity"]
            #     vehicle.total_capacity = veh_data["total_capacity"]

            # set fixed guideway times, and initial free flow auto link times
            # TODO: cntype_speed_map to config
            cntype_speed_map = {
                "CRAIL": 45.0,
                "HRAIL": 40.0,
                "LRAIL": 30.0,
                "FERRY": 15.0,
            }
            walk_speed = self.controller.config.transit.get("walk_speed", 3.0)
            transit_speed = self.controller.config.transit.get("transit_speed", 30.0)
            for link in network.links():
                speed = cntype_speed_map.get(link["#cntype"])
                if speed is None:
                    # speed = link["@free_flow_speed"]
                    speed = 30.0  # temp fix, will uncomment it when bring in highway changes
                    if link["@ft"] == 1 and speed > 0:
                        link["@trantime"] = 60 * link.length / speed
                    elif speed > 0:
                        link["@trantime"] = (
                            60 * link.length / speed + link.length * 5 * 0.33
                        )
                    else:
                        link["@trantime"] = 0
                else:
                    link["@trantime"] = 60 * link.length / speed
                link.data1 = link["@trantime"]
                # # set TAP connector distance to 60 feet
                # if link.i_node.is_centroid or link.j_node.is_centroid:
                #     link.length = 0.01  # 60.0 / 5280.0
            for line in network.transit_lines():
                # TODO: may want to set transit line speeds (not necessarily used in the assignment though)
                line_veh = network.transit_vehicle(
                    line["#vehtype"]
                )  # use #vehtype here instead of #mode (#vehtype is vehtype_num in Lasso\mtc_data\lookups\transitSeatCap.csv)
                if line_veh is None:
                    raise Exception(
                        f"line {line.id} requires vehicle ('#vehtype') {line['#vehtype']} which does not exist"
                    )
                line_mode = line_veh.mode.id
                for seg in line.segments():
                    seg.link.modes |= {line_mode}
                line.vehicle = line_veh
                # Set the perception factor from the mode table
                # IMPORTANT: Skip if mode not in config. This handles legacy network data issues
                # where transit lines may have been assigned highway modes (e.g., mode 'x' for MAZ-to-MAZ).
                # This was observed in sprint-04 (April 2025) networks created before November 2025
                # crosswalk validation improvements. Newer network builds should not have this issue.
                if line.vehicle.mode.id in in_vehicle_factors:
                    line["@invehicle_factor"] = in_vehicle_factors[line.vehicle.mode.id]
                    line["@iboard_penalty"] = initial_boarding_penalty[line.vehicle.mode.id]
                    line["@xboard_penalty"] = transfer_boarding_penalty[
                        line.vehicle.mode.id
                    ]
                else:
                    self.controller.logger.log(
                        f"Warning: Transit line {line.id} uses mode '{line.vehicle.mode.id}' not defined in "
                        f"transit.modes config. This may indicate network build issues. Skipping perception factors for this line.",
                        level="WARN"
                    )

            # # set link modes to the minimum set
            # auto_mode = {self.controller.config.highway.generic_highway_mode_code}
            # for link in network.links():
            #     # get used transit modes on link
            #     modes = {seg.line.mode for seg in link.segments()}
            #     # add in available modes based on link type
            #     if link["@drive_link"]:
            #         modes |= local_modes
            #         modes |= auto_mode
            #     if link["@bus_only"]:
            #         modes |= local_modes
            #     if link["@rail_link"] and not modes:
            #         modes |= premium_modes
            #     # add access, egress or walk mode (auxilary transit modes)
            #     if link.i_node.is_centroid:
            #         modes |= egress_modes
            #     elif link.j_node.is_centroid:
            #         modes |= access_modes
            #     elif link["@walk_link"]:
            #         modes |= walk_modes
            #     if not modes:  # in case link is unused, give it the auto mode
            #         link.modes = auto_mode
            #     else:
            #         link.modes = modes
            for link in network.links():
                # set default values
                link.i_node["@hdw_fraction"] = default_headway_fraction
                link.i_node["@wait_pfactor"] = default_transfer_wait_perception_factor
                link.i_node["@xboard_nodepen"] = 1
                link.j_node["@hdw_fraction"] = default_headway_fraction
                link.j_node["@wait_pfactor"] = default_transfer_wait_perception_factor
                link.j_node["@xboard_nodepen"] = 1
                # update modes on connectors - only if modes exist in network
                # Access mode 'a' and egress mode 'e' may not be defined in minimal transit configs
                if (link.i_node.is_centroid) and (link["@drive_link"] == 0):
                    if network.mode("a"):
                        link.modes = "a"
                elif (link.j_node.is_centroid) and (link["@drive_link"] == 0):
                    if network.mode("e"):
                        link.modes = "e"
                elif (link.i_node.is_centroid or link.j_node.is_centroid) and (
                    link["@drive_link"] != 0
                ):
                    link.modes = set([network.mode("c"), network.mode("D")])
                # calculate perceived walk time
                # perceived walk time will be used in walk mode definition "ul2",
                # link.data1 is used to save congested bus time, so use link.data2 here
                if link["@area_type"] in [0, 1]:
                    link.data2 = (
                        60 * link.length / (walk_speed / walk_perception_factor_cbd)
                    )
                else:
                    link.data2 = (
                        60 * link.length / (walk_speed / walk_perception_factor)
                    )

            # set headway fraction, transfer wait perception and transfer boarding penalty at specific nodes
            for line in network.transit_lines():
                for seg in line.segments():
                    seg.i_node["@hdw_fraction"] = headway_fraction[line.vehicle.mode.id]
                    seg.j_node["@hdw_fraction"] = headway_fraction[line.vehicle.mode.id]
                    seg.i_node["@wait_pfactor"] = transfer_wait_perception_factor[
                        line.vehicle.mode.id
                    ]
                    seg.j_node["@wait_pfactor"] = transfer_wait_perception_factor[
                        line.vehicle.mode.id
                    ]

                    if line.vehicle.mode.id == "h":
                        if (
                            seg.i_node["#node_id"]
                            in self.controller.config.transit.timed_transfer_nodes
                        ):
                            seg.i_node["@xboard_nodepen"] = 0

            ref_scenario.publish_network(network)

        self._prepare_scenarios_and_attributes(emmebank)

        with self.logger.log_start_end("remove transit lines from other periods"):
            for period in self.controller.config.time_periods:
                period_name = period.name.upper()
                with self.logger.log_start_end(f"period {period_name}"):
                    scenario = emmebank.scenario(period.emme_scenario_id)
                    network = scenario.get_network()
                    # removed transit lines from other periods from per-period scenarios
                    for line in network.transit_lines():
                        if line["#time_period"].upper() != period_name:
                            network.delete_transit_line(line)
                    scenario.publish_network(network)

    @LogStartEnd("Copy base to period scenarios and set per-period attributes")
    def _prepare_scenarios_and_attributes(self, emmebank):
        ref_scenario = emmebank.scenario(
            self.controller.config.emme.all_day_scenario_id
        )
        # self._project_coordinates(ref_scenario)
        # find all time-of-day attributes (ends with period name)
        tod_attr_groups = {
            "NODE": _defaultdict(lambda: []),
            "LINK": _defaultdict(lambda: []),
            "TURN": _defaultdict(lambda: []),
            "TRANSIT_LINE": _defaultdict(lambda: []),
            "TRANSIT_SEGMENT": _defaultdict(lambda: []),
        }
        for attr in ref_scenario.extra_attributes():
            for period in self.controller.config.time_periods:
                # Case-insensitive check: attribute names are lowercase (e.g., @lanes_am)
                # but period names may be uppercase (e.g., "AM")
                if attr.name.lower().endswith(period.name.lower()):
                    # Store with the original period name suffix length for extraction
                    tod_attr_groups[attr.type][attr.name[: -len(period.name)]].append(
                        attr.name
                    )
        
        # Debug: show what period attributes were found
        for domain, all_attrs in tod_attr_groups.items():
            if all_attrs:
                self.controller.logger.log(
                    f"DEBUG: Found {len(all_attrs)} time-of-day attribute groups for {domain}: {list(all_attrs.keys())[:5]}...",
                    level="INFO"
                )
        
        for period in self.controller.config.time_periods:
            scenario = emmebank.scenario(period.emme_scenario_id)
            if scenario:
                emmebank.delete_scenario(scenario)
            scenario = emmebank.copy_scenario(ref_scenario, period.emme_scenario_id)
            scenario.title = f"{period.name} {ref_scenario.title}"[:60]
            self.controller.logger.log(
                f"Created period scenario {period.emme_scenario_id} for {period.name}",
                level="DEBUG"
            )
            # in per-period scenario create attributes without period suffix, copy values
            # for this period and delete all other period attributes
            attrs_copied = []
            for domain, all_attrs in tod_attr_groups.items():
                for root_attr, tod_attrs in all_attrs.items():
                    # Build source attribute name - try lowercase period name first (e.g., @lanes_am)
                    # since EMME extra attribute names are typically lowercase
                    src_attr = f"{root_attr}{period.name.lower()}"
                    # If lowercase version doesn't exist, try the original case
                    if scenario.extra_attribute(src_attr) is None:
                        src_attr = f"{root_attr}{period.name}"
                    if root_attr.endswith("_"):
                        root_attr = root_attr[:-1]
                    for attr in tod_attrs:
                        if attr != src_attr:
                            scenario.delete_extra_attribute(attr)
                    # Create target attribute if it doesn't exist, otherwise use existing
                    if scenario.extra_attribute(root_attr) is None:
                        attr = scenario.create_extra_attribute(domain, root_attr)
                    else:
                        attr = scenario.extra_attribute(root_attr)
                    attr.description = scenario.extra_attribute(src_attr).description
                    values = scenario.get_attribute_values(domain, [src_attr])
                    scenario.set_attribute_values(domain, [root_attr], values)
                    scenario.delete_extra_attribute(src_attr)
                    attrs_copied.append(f"{src_attr}->{root_attr}")
            
            if attrs_copied:
                self.controller.logger.log(
                    f"Copied {len(attrs_copied)} period attributes: {attrs_copied[:5]}...",
                    level="DEBUG"
                )

    def _set_area_type(self, network):
        # set area type for links based on average density of MAZ closest to I or J node
        # the average density including all MAZs within the specified buffer distance
        buff_dist = 5280 * self.controller.config.highway.area_type_buffer_dist_miles
        maz_data_df = self.controller.maz_data
        maz_landuse_data: Dict[
            int, Dict[Any, Union[str, int, Tuple[float, float]]]
        ] = {}
        for index, row in maz_data_df.iterrows():
            row_dict = row.to_dict()
            maz_landuse_data[row_dict["MAZ_ORIGINAL"]] = row_dict        
        # Build spatial index of MAZ node coords
        sp_index_maz = SpatialGridIndex(size=0.5 * 5280)
        for node in network.nodes():
            if node["@maz_id"]:
                x, y = node.x, node.y
                maz_landuse_data[int(node["@maz_id"])]["coords"] = (x, y)
                sp_index_maz.insert(int(node["@maz_id"]), x, y)
        for maz_landuse in maz_landuse_data.values():
            x, y = maz_landuse.get("coords", (None, None))
            if x is None:
                continue  # some MAZs in table might not be in network
            # Find all MAZs with the square buffer (including this one)
            # (note: square buffer instead of radius used to match earlier implementation)
            other_maz_ids = sp_index_maz.within_square(x, y, buff_dist)
            # Sum total landuse attributes within buffer distance
            total_pop = sum(
                int(float(maz_landuse_data[maz_id]["POP"])) for maz_id in other_maz_ids
            )
            total_emp = sum(
                int(float(maz_landuse_data[maz_id]["emp_total"])) for maz_id in other_maz_ids
            )
            total_acres = sum(
                float(maz_landuse_data[maz_id]["ACRES"]) for maz_id in other_maz_ids
            )
            # calculate buffer area type
            if total_acres > 0:
                density = (1 * total_pop + 2.5 * total_emp) / total_acres
            else:
                density = 0
            # code area type class
            if density < 6:
                maz_landuse["area_type"] = 5  # rural
            elif density < 30:
                maz_landuse["area_type"] = 4  # suburban
            elif density < 55:
                maz_landuse["area_type"] = 3  # urban
            elif density < 100:
                maz_landuse["area_type"] = 2  # urban business
            elif density < 300:
                maz_landuse["area_type"] = 1  # cbd
            else:
                maz_landuse["area_type"] = 0  # regional core
        # Find nearest MAZ for each link, take min area type of i or j node
        for link in network.links():
            i_node, j_node = link.i_node, link.j_node
            a_maz = sp_index_maz.nearest(i_node.x, i_node.y)
            b_maz = sp_index_maz.nearest(j_node.x, j_node.y)
            link["@area_type"] = min(
                maz_landuse_data[a_maz]["area_type"],
                maz_landuse_data[b_maz]["area_type"],
            )

    @staticmethod
    def _set_capclass(network):
        for link in network.links():
            area_type = link["@area_type"]
            if area_type < 0:
                link["@capclass"] = -1
            elif link["@ft"] == 99:
                link["@capclass"] = 10 * area_type + 7
            else:
                link["@capclass"] = 10 * area_type + link["@ft"]

    def _set_speed(self, network):
        free_flow_speed_map = {}
        for row in self.controller.config.highway.capclass_lookup:
            if row.get("free_flow_speed") is not None:
                free_flow_speed_map[row["capclass"]] = row.get("free_flow_speed")
        for link in network.links():
            # default speed o 25 mph if missing or 0 in table map
            link["@free_flow_speed"] = free_flow_speed_map.get(link["@capclass"], 25)
            speed = link["@free_flow_speed"] or 25
            link["@free_flow_time"] = 60 * link.length / speed
