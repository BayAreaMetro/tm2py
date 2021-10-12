"""Performs equalibrium traffic assignment and generates resulting skims.

The traffic assignment runs according to the list of assignment classes
in the controller.config. Each classes is specified using the following
schema. All items are required unless indicated (no validation at present).
Note that some network preparation steps (such as the setting of link.modes)
are completed in the create_emme_network component.
    "name": short (e.g. 2-3 character) unique reference name for the class.
        used in attribute and matrix names
    "description": longer text used in attribute and matrix descriptions
    "mode_code": single character mode, used to generate link.modes to
        identify subnetwork, generated from "exclued_links" keywords
    "demand": list of OMX file and matrix keyname references
        "source": reference name of the component section for the
            source "highway_demand_file" location
        "name": name of matrix in the OMX file, can include "{period}"
            placeholder
        "factor": optional, multiplicative factor to generate PCEs from
            trucks or convert person-trips to vehicle-trips for HOVs
    "excluded_links": list of keywords to identify links to exclude from
        this class' available subnetwork (generate link.modes)
        Options are:
            - "is_toll_da": has a value (non-bridge) toll for drive alone
            - "is_sr2": is reserved for shared ride 2+ (@useclass in 2,3)
            - "is_sr3": is reserved for shared ride 3+ (@useclass == 3)
            - "is_toll_sr2": has a value (non-bridge) toll for shared ride 2
            - "is_toll_sr3": has a value (non-bridge) toll for shared ride 3+
            - "is_toll_truck": has a value (non-bridge) toll for trucks
            - "is_auto_only": is reserved for autos (non-truck) (@useclass != 1)
    "value_of_time": value of time for this class in $ / hr
    "operating_cost_per_mile": vehicle operating cost in cents / mile
    "toll": additional toll cost link attribute (in cents)
    "toll_factor": optional, factor to apply to toll values in cost calculation
    "pce": optional, passenger car equivalent to convert assigned demand in
        PCE units to vehicles for total assigned vehicle calculations
    "skims": list of skim matrices to generate
        Options are:
            "time": travel time only in minutes
            "dist": distance in miles
            "hovdist": distance on HOV (sr2 or sr3+) facilities
            "tolldist": distance on toll (@valuetoll_da > 0) facilities
            "freeflowtime": free flow travel time in minutes
            "bridgetoll_YY": bridge tolls, where YY is one of the class groups
            "valuetoll_YY": other, non-bridge tolls, where YY is one of the class groups

The available class groups for the skim / attribute names are:
"da", "sr2", "sr3", "vsm", sml", "med", "lrg"

Example single class config, as a Python dictionary:
    {
        "name": "da",
        "description": "drive alone",
        "mode_code": "d",
        "demand": [
            {"source": "household", "name": "SOV_GP_{period}"},
            {"source": "air_passenger", "name": "DA"},
            {"source": "internal_external", "name": "DA"},
        ],
        "excluded_links": ["is_toll_da", "is_sr2"],
        "value_of_time": 18.93,  # $ / hr
        "operating_cost_per_mile": 17.23,  # cents / mile
        "toll": "@bridgetoll_da",
        "skims": ["time", "dist", "freeflowtime", "bridgetoll_da"],
    }

Other relevant parameters from the config are
    highway.relative_gap: target relative gap stopping criteria
    highway.max_iterations: maximum iterations stopping criteria
    emme.num_processors: number of processors as integer or "MAX" or "MAX-N"
    periods[].emme_scenario_id: Emme scenario number to use for each period

The Emme network must have the following attributes available:
    Link:
    - "length" in feet
    - "type", the facility type
    - "vdf", volume delay function (volume delay functions must also be setup)
    - "@useclass", vehicle-class restrictions classification, auto-only, HOV only
    - "@free_flow_time", the free flow time (in minutes)
    - "@tollXX_YY", the toll for period XX and class subgroup (see truck
        class) named YY, used together with @tollbooth to generate @bridgetoll_YY
        and @valuetoll_YY
    - "@tollbooth", label to separate bridgetolls from valuetolls
    - modes: must be set on links and match the specified mode codes in
        the traffic config
    Node:
    - "@mazseq": the MAZ identifiers (used in the highwaymaz assignment)
    - "#county": the county name

 Network results:
    - "@bridgetoll_YY", the bridge toll for period XX and class subgroup
        (see truck class) named YY
    - "@valuetoll_YY", the "value", non-bridge toll for period XX and class
        subgroup (see truck class) named YY
    - @flow_YY: link PCE flows per class, where YY is the class name in the config
    - timau: auto travel time
    - volau: total assigned flow in PCE

 Notes:
    - Output matrices are in miles, minutes, and cents (2010 dollars) and are stored as real values;
    - Intrazonal distance/time is one half the distance/time to the nearest neighbor;
    - Intrazonal bridge and value tolls are assumed to be zero

The following are the default example setup which are specified
in the config.properties file.
    The traffic assignment is run for the following periods:
        (1) EA: early AM, 3 am to 6 am;
        (2) AM: AM peak period, 6 am to 10 am;
        (3) MD: midday, 10 am to 3 pm;
        (4) PM: PM peak period, 3 pm to 7 pm; and,
        (5) EV: evening, 7 pm to 3 am.

    Ten vehicle classes are tracked in each assignment, with value-toll-eligible
    and not-value-toll-eligible classes, with the skims noted below.
        (1) da, mode "d", drive alone, no value toll
                skims: time, dist, freeflowtime, bridgetoll_da
        (2) sr2, mode "e", shared ride 2, no value toll
                skims: time, dist, freeflowtime, bridgetoll_sr2, hovdist
        (3) sr3, mode "f", shared ride 3+, no value toll
                skims: time, dist, freeflowtime, bridgetoll_sr3, hovdist
        (4) trk, mode "t", very small, small, and medium trucks, no value toll
                skims: time, dist, freeflowtime,
                       bridgetollvsm, bridgetollsml, bridgetollmed
        (5) lrgtrk, mode "l", large trucks, no value toll
                skims: time, dist, freeflowtime, bridgetoll_lrg
        (6) datoll, mode "D", drive alone, value toll eligible
                skims: time, dist, freeflowtime, bridgetoll_da, valuetoll_da, tolldist
        (7) sr2toll, mode "E", shared ride 2, value toll eligible
                skims: time, dist, freeflowtime, bridgetoll_sr2, valuetoll_sr2, tolldist
        (8) sr3toll, mode "F", shared ride 3+, value toll eligible
                skims: time, dist, freeflowtime, bridgetoll_sr3, valuetoll_sr3, tolldist
        (9) trktoll, mode "T", very small, small, and medium trucks, value toll eligible
                skims: time, dist, freeflowtime,
                       bridgetollvsm, bridgetollsml, bridgetollmed,
                       valuetollvsm, valuetollsml, valuetollmed
        (10) lrgtrktoll, mode "L", large trucks, value toll eligible
                skims: time, dist, freeflowtime,

    Note that the "truck" and "trucktoll" classes combine very small, small and
    medium trucks

    The skims are stored in the Emmebank and exported to OMX with names with
    the following convention:
        period_class_skim, e.g. am_da_bridgetoll_da

    Four types of trips are assigned:
        (a) personal, inter-regional travel, file "household";
        (b) personal, intra-regional travel, file "internal_external";
        (c) commercial travel, file "commercial";
        (d) air passenger travel, file "air_passenger";

    The trip tables are read in by the script for each of these
    travel types.
"""

from contextlib import contextmanager as _context
import os
from typing import Dict, Any, Tuple, Union

import numpy as np
from tm2py.core.component import Component as _Component, Controller as _Controller
import tm2py.core.emme as _emme_tools
from tm2py.core.logging import LogStartEnd
from tm2py.core.tools import SpatialGridIndex


class HighwayAssignment(_Component):
    """Highway assignment and skims"""

    def __init__(self, controller: _Controller):
        """Highway assignment and skims.

        Args:
            controller: parent Controller object
        """
        super().__init__(controller)
        self._num_processors = _emme_tools.parse_num_processors(
            self.config.emme.num_processors
        )
        self._matrix_cache = None
        self._emme_manager = None
        self._emmebank = None
        self._skim_matrices = []

    @property
    def _modeller(self):
        return self._emme_manager.modeller

    @LogStartEnd("highway assignment and skims")
    def run(self):
        """Run highway assignment and skims."""
        project_path = os.path.join(self.root_dir, self.config.emme.project_path)
        self._emme_manager = _emme_tools.EmmeManager()
        emme_app = self._emme_manager.project(project_path)
        self._emme_manager.init_modeller(emme_app)
        emmebank_path = os.path.join(self.root_dir, self.config.emme.highway_database_path)
        self._emmebank = emmebank = self._emme_manager.emmebank(emmebank_path)
        # Run assignment and skims for all specified periods
        for period in self.config.periods:
            with self.logger.log_start_end(f"period {period.name}"):
                scenario_id = period.emme_scenario_id
                scenario = emmebank.scenario(scenario_id)
                with self._setup(scenario):
                    if self.controller.iteration > 0:
                        # Import demand from specified OMX files
                        # Will also MSA average demand if iteration > 1
                        import_demand = ImportDemand(
                            self.controller, scenario, period.name
                        )
                        import_demand.run()
                    else:
                        matrix = emmebank.matrix('ms"zero"')
                        if matrix:
                            emmebank.delete_matrix(matrix)
                        ident = emmebank.available_matrix_identifier("SCALAR")
                        matrix = emmebank.create_matrix(ident)
                        matrix.name = 'zero'
                        matrix.description = "Zero value matrix for FF assign"

                        self._prepare_network(scenario, period)

                    self._assign_and_skim(period.name, scenario)
                    self._export_skims(period.name, scenario)
                    if self.config.scenario.verify and self.controller.iteration == 1:
                        self._verify(period.name, scenario)

    @_context
    def _setup(self, scenario):
        self._matrix_cache = _emme_tools.MatrixCache(scenario)
        self._skim_matrices = []
        with self._emme_manager.logbook_trace("Traffic assignments"):
            try:
                yield
            finally:
                self._matrix_cache.clear()
                self._matrix_cache = None

    @LogStartEnd("prepare network attributes and modes")
    def _prepare_network(self, scenario, period):
        network = scenario.get_network()
        attributes = {
            "LINK": ["@capclass", "@area_type", "@capacity", "@free_flow_speed", "@free_flow_time", "@ja"]
        }
        # toll field attributes in scenario and network object
        dst_veh_groups = self.config.highway.tolls.dst_vehicle_group_names
        for dst_veh in dst_veh_groups:
            for toll_type in "bridge", "value":
                attributes["LINK"].append(f"@{toll_type}toll_{dst_veh}")

        for domain, attrs in attributes.items():
            for name in attrs:
                if name in network.attributes("LINK"):
                    network.delete_attribute("LINK", name)
                network.create_attribute("LINK", name)
                if scenario.extra_attribute(name) is None:
                    scenario.create_extra_attribute("LINK", name)

        self._set_tolls(network, period)
        self._set_area_type(network)
        self._set_capclass(network)
        self._set_speed(network)
        self._set_vdf_attributes(network, period)
        self._set_link_modes(network)
        scenario.publish_network(network)

    def _set_area_type(self, network):
        # set area type for links based on average density of MAZ closest to I or J node
        # the average density including all MAZs within the specified buffer distance
        buff_dist = 5280 * self.config.highway.area_type_buffer_dist_miles
        maz_data_file_path = os.path.join(self.root_dir, self.config.scenario.maz_landuse_file)
        maz_landuse_data: Dict[int, Dict[Any, Union[str, int, Tuple[float, float]]]] = {}
        with open(maz_data_file_path, 'r') as maz_data_file:
            header = [h.strip() for h in next(maz_data_file).split(",")]
            for line in maz_data_file:
                data = dict(zip(header, line.split(",")))
                maz_landuse_data[int(data["MAZ_ORIGINAL"])] = data
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
            total_pop = sum(int(maz_landuse_data[maz_id]["POP"]) for maz_id in other_maz_ids)
            total_emp = sum(int(maz_landuse_data[maz_id]["emp_total"]) for maz_id in other_maz_ids)
            total_acres = sum(float(maz_landuse_data[maz_id]["ACRES"]) for maz_id in other_maz_ids)
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
                maz_landuse_data[b_maz]["area_type"]
            )

    def _set_capclass(self, network):
        for link in network.links():
            area_type = link["@area_type"]
            if area_type < 0:
                link["@capclass"] = -1
            else:
                link["@capclass"] = 10 * area_type + link["@ft"]

    def _set_speed(self, network):
        free_flow_speed_map = {}
        for row in self.config.model.highway.capclass_lookup:
            if row.get("free_flow_speed") is not None:
                free_flow_speed_map[row["capclass"]] = row.get("free_flow_speed")
        for link in network.links():
            # default speed o 25 mph if missing or 0 in table map
            link["@free_flow_speed"] = free_flow_speed_map.get(link["@capclass"], 25)
            speed = link["@free_flow_speed"] or 25
            link["@free_flow_time"] = 60 * link.length / speed

    def _set_tolls(self, network, period):
        # TODO: validate format of tolls.csv file
        # TODO: report on tolls
        toll_file_path = os.path.join(self.root_dir, self.config.highway.tolls.file_path)
        tolls = {}
        with open(toll_file_path, 'r') as toll_file:
            header = next(toll_file).split(",")
            for line in toll_file:
                data = dict(zip(header, line.split(",")))
                tolls[data["fac_index"]] = data

        src_veh_groups = self.config.highway.tolls.src_vehicle_group_names
        dst_veh_groups = self.config.highway.tolls.dst_vehicle_group_names

        tollbooth_start_index = self.config.highway.tolls.tollbooth_start_index
        pname = period.name.lower()
        for link in network.links():
            tollbooth = link["@tollbooth"]
            if tollbooth:
                index = link["@tollbooth"] * 1000 + link["@tollseg"] * 10 + link[f"@useclass"]
                data_row = tolls.get(index)
                if data_row is None:
                    # TODO: report on failed lookup, may want to have optional halt model in config
                    continue  # tolls will remain at zero
                # if index is below tollbooth start index then this is a bridge (point toll), available
                # for all traffic assignment classes
                if link["@tollbooth"] < tollbooth_start_index:
                    for src_veh, dst_veh in zip(src_veh_groups, dst_veh_groups):
                        link[f"@bridgetoll_{dst_veh}"] = data_row[f"toll{pname}_{src_veh}"] * 100
                # else, this is a tollway with a per-mile charge
                else:
                    for src_veh, dst_veh in zip(src_veh_groups, dst_veh_groups):
                        link[f"@valuetoll_{dst_veh}"] = data_row[f"toll{pname}_{src_veh}"] * link.length * 100

    def _set_vdf_attributes(self, network, period):
        # Set capacity, VDF and critical speed on links
        capacity_map = {}
        critical_speed_map = {}
        for row in self.config.model.highway.capclass_lookup:
            if row.get("capacity") is not None:
                capacity_map[row["capclass"]] = row.get("capacity")
            if row.get("critical_speed") is not None:
                critical_speed_map[row["capclass"]] = row.get("critical_speed")
        period_capacity_factor = period.highway_capacity_factor
        akcelik_vdfs = [3, 4, 5, 7, 8, 10, 11, 12, 13, 14]
        for link in network.links():
            cap_lanehour = capacity_map[link["@capclass"]]
            link["@capacity"] = cap_lanehour * period_capacity_factor * link["@lanes"]
            link.volume_delay_func = int(link["@ft"])
            # re-mapping links with type 99 to type 7 "local road of minor importance"
            if link.volume_delay_func == 99:
                link.volume_delay_func = 7
            # num_lanes not used directly, but set for reference
            link.num_lanes = max(min(9.9, link["@lanes"]), 1.0)
            if link.volume_delay_func in akcelik_vdfs and link["@free_flow_speed"] > 0:
                dist = link.length
                CritSpd = critical_speed_map[link["@capclass"]]
                Tc = (dist / CritSpd)
                To = (dist / link["@free_flow_speed"])
                link["@ja"] = 16 * (Tc - To) ** 2

    def _set_link_modes(self, network):
        # first reset link modes (script run more than once)
        # "generic_highway_mode_code" must already be created (in import to Emme script)
        auto_mode = network.mode(self.config.highway.generic_highway_mode_code)
        for link in network.links():
            modes = [m for m in link.modes if m.type in ["TRANSIT", "AUX_TRANSIT"]]
            modes.append(auto_mode)
            link.modes = modes
        for mode in network.modes():
            if mode.type == "AUX_AUTO":
                network.delete_mode(mode)

        # create modes from class spec
        # (duplicate mode codes allowed provided the excluded_links is the same)
        mode_excluded_links = {}
        for assign_class in self.config.highway.classes:
            if network.mode(assign_class.mode_code):
                if assign_class.excluded_links != mode_excluded_links[assign_class.mode_code]:
                    ex_links1 = mode_excluded_links[assign_class.mode_code]
                    ex_links2 = assign_class.excluded_links
                    raise Exception(
                        f"config error: highway.classes, duplicated mode codes ('{assign_class.mode_code}')"
                        f" with different excluded links: {ex_links1} and {ex_links2}")
                else:
                    continue
            mode = network.create_mode("AUX_AUTO", assign_class.mode_code)
            mode.description = assign_class.name
            mode_excluded_links[mode.id] = assign_class.excluded_links

        # TODO: ok like this for now, but should consider moving to highway_maz script
        maz_access_mode = network.create_mode("AUX_AUTO", self.config.highway.maz_to_maz.mode_code)
        maz_access_mode.description = "MAZ access"

        def apply_exclusions(excluded_links_criteria, mode_code, modes_set, link_values):
            for criteria in excluded_links_criteria:
                if link_values[criteria]:
                    return
            modes_set.add(mode_code)

        dst_veh_groups = self.config.highway.tolls.dst_vehicle_group_names
        for link in network.links():
            modes = set([m.id for m in link.modes])
            if link.i_node["@maz_id"] + link.j_node["@maz_id"] > 0:
                # MAZ connectors, special MAZ access / egress mode
                modes.add(maz_access_mode.id)
            else:
                exclude_links_map = {
                    "is_sr": link["@useclass"] in [2, 3],
                    "is_sr3": link["@useclass"] == 3,
                    "is_auto_only": link["@useclass"] in [2, 3, 4],
                }
                for dst_veh in dst_veh_groups:
                    exclude_links_map[f"is_toll_{dst_veh}"] = link[f"@valuetoll_{dst_veh}"] > 0
                for assign_class in self.config.highway.classes:
                    apply_exclusions(assign_class.excluded_links, assign_class.mode_code, modes, exclude_links_map)
                apply_exclusions(
                    self.config.highway.maz_to_maz.excluded_links,
                    maz_access_mode.id,
                    modes, exclude_links_map)
            link.modes = modes

    def _assign_and_skim(self, period, scenario):
        """Runs Emme SOLA assignment with path analyses (skims)."""
        traffic_assign = self._modeller.tool(
            "inro.emme.traffic_assignment.sola_traffic_assignment"
        )
        create_attribute = self._modeller.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        net_calc = _emme_tools.NetworkCalculator(scenario)

        with self._emme_manager.logbook_trace(f"Prepare scenario for period {period}"):
            # prepare network attributes for skimming
            create_attribute(
                "LINK", "@hov_length", "length HOV lanes", overwrite=True, scenario=scenario
            )
            net_calc("@hov_length", "length * (@useclass >= 2 && @useclass <= 3)")
            create_attribute(
                "LINK", "@toll_length", "length tolls", overwrite=True, scenario=scenario
            )
            # non-bridge toll facilities
            tollbooth_start_index = self.config.highway.tolls.tollbooth_start_index
            net_calc("@toll_length", f"length * (@tollbooth >= {tollbooth_start_index})")
            # create Emme format specification with traffic class definitions
            # and path analyses (skims)
            assign_spec = self._base_spec()
            for class_config in self.config.highway.classes:
                emme_class_spec = self._prepare_traffic_class(
                    class_config, scenario, period
                )
                assign_spec["classes"].append(emme_class_spec)

        # Run assignment
        traffic_assign(assign_spec, scenario, chart_log_interval=1)

        # Subtrack the non-time costs from generalize cost to get the raw travel time skim
        for emme_class_spec in assign_spec["classes"]:
            self._calc_time_skim(emme_class_spec)
        # Set intra-zonals for time and dist to be 1/2 nearest neighbour
        for class_config in self.config.highway.classes:
            self._set_intrazonal_values(
                period, class_config["name"], class_config["skims"]
            )

    def _calc_time_skim(self, emme_class_spec):
        """Cacluate the matrix skim time=gen_cost-per_fac*link_costs"""
        od_travel_times = emme_class_spec["results"]["od_travel_times"][
            "shortest_paths"
        ]
        if od_travel_times is not None:
            # Total link costs is always the first analysis
            cost = emme_class_spec["path_analyses"][0]["results"]["od_values"]
            factor = emme_class_spec["generalized_cost"]["perception_factor"]
            gencost_data = self._matrix_cache.get_data(od_travel_times)
            cost_data = self._matrix_cache.get_data(cost)
            time_data = gencost_data - (factor * cost_data)
            self._matrix_cache.set_data(od_travel_times, time_data)

    def _set_intrazonal_values(self, period, class_name, skims):
        """Set the intrazonal values to 1/2 nearest neighbour for time and distance skims."""
        for skim_name in skims:
            if skim_name in ["time", "distance", "freeflowtime", "hovdist", "tolldist"]:
                matrix_name = f'mf"{period}_{class_name}_{skim_name}"'
                matrix = self._emmebank.matrix(matrix_name)
                if not matrix:
                    raise Exception(f"Matrix {matrix_name} does not exist")
                data = self._matrix_cache.get_data(matrix)
                # NOTE: sets values for external zones as well
                np.fill_diagonal(data, np.inf)
                data[np.diag_indices_from(data)] = 0.5 * np.nanmin(data, 1)
                self._matrix_cache.set_data(matrix, data)

    @LogStartEnd()
    def _export_skims(self, period, scenario):
        """Export skims to OMX files by period."""
        # NOTE: skims in separate file by period
        omx_file_path = os.path.join(
            self.root_dir,
            self.config.highway.output_skim_path.format(period=period))
        os.makedirs(os.path.dirname(omx_file_path), exist_ok=True)
        with _emme_tools.OMX(
            omx_file_path, "w", scenario, matrix_cache=self._matrix_cache
        ) as omx_file:
            omx_file.write_matrices(self._skim_matrices)
        self._skim_matrices = []
        self._matrix_cache.clear()

    def _base_spec(self):
        """Generate template Emme SOLA assignment specification"""
        relative_gap = self.config.highway.relative_gap
        max_iterations = self.config.highway.max_iterations
        # NOTE: mazmazvol as background traffic in link.data1 ("ul1")
        base_spec = {
            "type": "SOLA_TRAFFIC_ASSIGNMENT",
            "background_traffic": {
                "link_component": "ul1",
                "turn_component": None,
                "add_transit_vehicles": False,
            },
            "classes": [],
            "stopping_criteria": {
                "max_iterations": max_iterations,
                "best_relative_gap": 0.0,
                "relative_gap": relative_gap,
                "normalized_gap": 0.0,
            },
            "performance_settings": {"number_of_processors": self._num_processors},
        }
        return base_spec

    def _prepare_traffic_class(self, class_config, scenario, period):
        """Prepare attributes and matrices and path analyses specs by class."""
        create_attribute = self._modeller.tool(
            "inro.emme.data.extra_attribute.create_extra_attribute"
        )
        net_calc = _emme_tools.NetworkCalculator(scenario)

        name = class_config["name"]
        name_lower = name.lower()
        op_cost = class_config["operating_cost_per_mile"]
        toll = class_config["toll"]
        toll_factor = class_config.get("toll_factor")
        create_attribute(
            "LINK",
            f"@cost_{name_lower}",
            f'{period} {class_config["description"]} total costs'[:40],
            overwrite=True,
            scenario=scenario,
        )
        if toll_factor is None:
            cost_expression = f"length * {op_cost} + {toll}"
        else:
            cost_expression = f"length * {op_cost} + {toll} * {toll_factor}"
        net_calc(f"@cost_{name_lower}", cost_expression)
        create_attribute(
            "LINK",
            f"@flow_{name_lower}",
            f'{period} {class_config["description"]} link volume'[:40],
            0,
            overwrite=True,
            scenario=scenario,
        )

        class_analysis, od_travel_times = self._prepare_path_analyses(
            class_config["skims"], scenario, period, name
        )
        if self.controller.iteration == 0:
            demand_matrix = 'ms"zero"'
        else:
            demand_matrix = f'mf"{period}_{name}"'
        emme_class_spec = {
            "mode": class_config["mode_code"],
            "demand": demand_matrix,
            "generalized_cost": {
                "link_costs": f"@cost_{name_lower}",  # cost in $0.01
                # $/hr -> min/$0.01
                "perception_factor": 0.6 / class_config["value_of_time"],
            },
            "results": {
                "link_volumes": f"@flow_{name_lower}",
                "od_travel_times": {"shortest_paths": f'mf"{od_travel_times}"'},
            },
            "path_analyses": class_analysis,
        }
        return emme_class_spec

    def _prepare_path_analyses(self, skim_names, scenario, period, name):
        """Prepare the path analysis specification and matrices for all skims"""
        create_matrix = self._modeller.tool("inro.emme.data.matrix.create_matrix")
        skim_names = skim_names[:]
        skim_matrices = []
        class_analysis = []
        # time skim is special case, get total generialized cost and link costs
        # then calculate time = gen_cost - (oper_cost + toll)
        if "time" in skim_names:
            # total generalized cost results from od_travel_time
            od_travel_times = f"{period}_{name}_time"
            skim_matrices.append(od_travel_times)
            # also get non-time costs
            skim_matrices.append(f"{period}_{name}_cost")
            class_analysis.append(
                self._analysis_spec(f"{period}_{name}_cost", f"@cost_{name}".lower())
            )
            skim_names.remove("time")
        else:
            od_travel_times = None

        for skim_type in skim_names:
            if "_" in skim_type:
                skim_type, group = skim_type.split("_")
            else:
                group = ""
            analysis_link = {
                "dist": "length",  # NOTE: length must be in miles
                "hovdist": "@hov_length",
                "tolldist": "@toll_length",
                "freeflowtime": "@free_flow_time",
                "bridgetoll": f"@bridgetoll_{group}",
                "valuetoll": f"@valuetoll_{group}",
            }
            if group:
                matrix_name = f"{period}_{name}_{skim_type}{group}"
            else:
                matrix_name = f"{period}_{name}_{skim_type}"
            class_analysis.append(
                self._analysis_spec(matrix_name, analysis_link[skim_type])
            )
            skim_matrices.append(matrix_name)

        # create / initialize skim matrices
        for matrix_name in skim_matrices:
            matrix = self._emmebank.matrix(f'mf"{matrix_name}"')
            if not matrix:
                matrix = create_matrix("mf", matrix_name, scenario=scenario, overwrite=True)
            self._skim_matrices.append(matrix)

        return class_analysis, od_travel_times

    @staticmethod
    def _analysis_spec(matrix_name, link_attr):
        """Template path analysis spec"""
        analysis_spec = {
            "link_component": link_attr,
            "turn_component": None,
            "operator": "+",
            "selection_threshold": {"lower": None, "upper": None},
            "path_to_od_composition": {
                "considered_paths": "ALL",
                "multiply_path_proportions_by": {
                    "analyzed_demand": False,
                    "path_value": True,
                },
            },
            "results": {
                "od_values": f'mf"{matrix_name}"',
                "selected_link_volumes": None,
                "selected_turn_volumes": None,
            },
        }
        return analysis_spec

    def _verify(self, period, scenario):
        """Run post-process verification steps"""
        # calc_vmt
        net_calc = _emme_tools.NetworkCalculator(scenario)
        class_vehs = []
        for class_config in self.config.highway.classes:
            name = class_config.name.lower()
            pce = class_config.get("pce", 1.0)
            class_vehs.append(f"@flow_{[name]}*{[pce]}")
        if not scenario.extra_attribute("@total_vehicles"):
            scenario.create_extra_attribute("LINK", "@total_vehicles")
        net_calc.add_calc("@total_vehicles", "+".join(class_vehs))
        net_calc.add_calc(result=None, expression="length * @total_vehicles")
        reports = net_calc.run()
        total_vmt = reports[1]["sum"]
        # TODO: specifiy acceptable VMT range, could come from config
        # min_vmt = {"ea": ?}
        # max_vmt = {"ea": ?}
        assert min_vmt[period] <= total_vmt <= max_vmt[period]

        # check all skim matrices for infinities
        errors = []
        for matrix in self._skim_matrices:
            data = self._matrix_cache.get_data(matrix)
            if not (data < 1e19).all():
                errors.append(f"{matrix.name} has infinite (>1e19) values")
        # assert no error message has been registered, else print messages
        assert not errors, "errors occured:\n{}".format("\n".join(errors))


# TODO: import demand to separate python file
# TODO: incorporate import of transit demand

class ImportDemand(_Component):
    """Import and average highway assignment demand from OMX files to Emme database"""

    def __init__(
        self,
        controller: _Controller,
        scenario: _emme_tools.EmmeScenario,
        period: str,
    ):
        """Import and average highway demand.

        Demand is imported from OMX files based on reference file paths and OMX
        matrix names in highway assignment config (highway.classes).
        The demand is average using MSA with the current demand matrices if the
        controller.iteration > 1.

        Args:
            controller: parent Controller object
            root_dir (str): root directory containing Emme project, demand matrices
            scenario: Emme scenario object for reference zone system
            period: time period ID
        """
        super().__init__(controller)
        self._scenario = scenario
        self._period = period
        self._omx_files = {}

    @LogStartEnd("import highway demand")
    def run(self):
        """Run demand import from OMX files and average"""
        scenario = self._scenario
        period = self._period
        traffic_config = self.config.highway.classes
        msa_iteration = self.controller.iteration
        emmebank = scenario.emmebank
        num_zones = len(scenario.zone_numbers)
        with self._setup():
            for class_config in traffic_config:
                # sum up demand from all sources (listed in config)
                demand = self._read_demand(class_config["demand"][0], num_zones)
                for file_config in class_config["demand"][1:]:
                    demand = demand + self._read_demand(file_config, num_zones)
                # get the Emme matrix, create a new matrix if needed
                demand_name = f'{period}_{class_config["name"]}'
                matrix = emmebank.matrix(f'mf"{demand_name}"')
                if msa_iteration <= 1:
                    if not matrix:
                        ident = emmebank.available_matrix_identifier("FULL")
                        matrix = emmebank.create_matrix(ident)
                        matrix.name = demand_name
                    matrix.description = (
                        f'{period} {class_config["description"]} demand'
                    )
                else:
                    if not matrix:
                        raise Exception(f"{demand_name} matrix does not exist in Emmebank, cannot use MSA iteration {msa_iteration}")
                    # Load prev demand and MSA average
                    prev_demand = matrix.get_numpy_data(scenario.id)
                    demand = prev_demand + (1.0 / msa_iteration) * (
                        demand - prev_demand
                    )
                matrix.set_numpy_data(demand, scenario.id)

    @_context
    def _setup(self):
        self._omx_files = {}
        try:
            yield
        finally:
            for file_obj in self._omx_files.values():
                file_obj.close()
            self._omx_files = {}

    def _read_demand(self, file_config, num_zones):
        source = file_config["source"]
        name = file_config["name"].format(period=self._period.upper())
        factor = file_config.get("factor")
        file_obj = self._omx_files.get(source)
        if not file_obj:
            # REVIEW: should source reference the full key instead of
            #         fixed "highway_demand_file" ?
            path = self.config[source].highway_demand_file
            file_obj = _emme_tools.OMX(
                os.path.join(self.root_dir, path.format(period=self._period))
            )
            file_obj.open()
            self._omx_files[source] = file_obj
        demand = file_obj.read(name)
        if factor is not None:
            demand = factor * demand
        shape = demand.shape
        # pad external zone values with 0
        if shape != (num_zones, num_zones):
            demand = np.pad(
                demand, ((0, num_zones - shape[0]), (0, num_zones - shape[1]))
            )
        return demand
