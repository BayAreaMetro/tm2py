"""Module for creating the time-of-day scenario.
"""
from __future__ import annotations

from typing import Dict, Any, Union, List

from tm2py.components.component import Component
from tm2py.emme.manager import EmmeNode, EmmeNetwork
from tm2py.emme.network import SpatialGridIndex, copy_tod_scenario
from tm2py.logger import LogStartEnd


class CreateHighwayScenarios(Component):
    """Create the per-time period scenarios for highway"""

    @LogStartEnd("Create highway time of day scenarios.")
    def run(self):
        emmebank_path = self.get_abs_path(self.config.emme.highway_database_path)
        manager = self.controller.emme_manager
        emmebank = manager.emmebank(emmebank_path)
        # Note for future: may need to update extra attribute values
        manager.change_emmebank_dimensions(
            emmebank, {"full_matrices": 9999, "extra_attribute_values": 60000000}
        )
        self._create_auto_functions(emmebank)
        ref_scenario = emmebank.scenario(self.config.emme.all_day_scenario_id)
        attributes = {
            "LINK": ["@area_type", "@capclass", "@free_flow_speed", "@free_flow_time"]
        }
        for domain, attrs in attributes.items():
            for name in attrs:
                if ref_scenario.extra_attribute(name) is None:
                    ref_scenario.create_extra_attribute(domain, name)

        network = ref_scenario.get_network()
        self._set_area_type(network)
        self._set_capclass(network)
        self._set_speed(network)
        ref_scenario.publish_network(network)

        all_period_names = self.time_period_names()
        for period in self.config.time_periods:
            copy_tod_scenario(
                ref_scenario, period.name, period.emme_scenario_id, all_period_names
            )

    @staticmethod
    def _create_auto_functions(emmebank):
        """create VDFs & set cross-reference function parameters"""
        emmebank.extra_function_parameters.el1 = "@free_flow_time"
        emmebank.extra_function_parameters.el2 = "@capacity"
        emmebank.extra_function_parameters.el3 = "@ja"
        # Note: could use optimize expression (commented out below)
        bpr_tmplt = "el1 * (1 + 0.20 * ((volau + volad)/el2/0.75))^6"
        # "el1 * (1 + 0.20 * put(put((volau + volad)/el2/0.75))*get(1))*get(2)*get(2)"
        fixed_tmplt = "el1"
        akcelik_tmplt = (
            "(el1 + 60 * (0.25 *((((volau + volad)/el2) - 1) + "
            "((((((volau + volad)/el2) - 1)^2) + (16 * el3 * ("
            "(volau + volad)/el2)))^0.5))))"
            # "(el1 + 60 * (0.25 *(put(put((volau + volad)/el2) - 1) + "
            # "(((get(2)*get(2) + (16 * el3 * get(1)^0.5))))"
        )
        for f_id in ["fd1", "fd2", "fd9"]:
            if emmebank.function(f_id):
                emmebank.delete_function(f_id)
            emmebank.create_function(f_id, bpr_tmplt)
        for f_id in [
            "fd3",
            "fd4",
            "fd5",
            "fd7",
            "fd8",
            "fd10",
            "fd11",
            "fd12",
            "fd13",
            "fd14",
        ]:
            if emmebank.function(f_id):
                emmebank.delete_function(f_id)
            emmebank.create_function(f_id, akcelik_tmplt)
        if emmebank.function("fd6"):
            emmebank.delete_function("fd6")
        emmebank.create_function("fd6", fixed_tmplt)

    def _set_area_type(self, network: EmmeNetwork):
        # set area type for links based on average density of MAZ closest to I or J node
        # the average density including all MAZs within the specified buffer distance
        buff_dist = 5280 * self.config.highway.area_type_buffer_dist_miles
        maz_landuse_data = self._load_maz_landuse_data()
        # Build spatial index of MAZ node coords
        sp_index_maz = SpatialGridIndex(size=0.5 * 5280)
        node_index = dict((node["@maz_id"], node) for node in network.nodes() if node["@maz_id"] != 0)
        for maz_id, maz_landuse in maz_landuse_data.items():
            node = maz_landuse["node"] = node_index.get(maz_id)
            if node is None:
                continue  # some MAZs in table might not be in network
            sp_index_maz.insert(maz_id, node.x, node.y)
        for maz_landuse in maz_landuse_data.values():
            node = maz_landuse["node"]
            if node is None:
                continue  # some MAZs in table might not be in network
            # Find all MAZs with the square buffer (including this one)
            # (note: square buffer instead of radius used to match earlier implementation.
            #  It would be more logical to use radius but it would likely not make much
            #  difference)
            selected_maz_ids = sp_index_maz.within_square(node.x, node.y, buff_dist)
            # Sum total landuse attributes within buffer distance
            maz_landuse["area_type"] = self._calc_maz_area_type(
                maz_landuse_data, selected_maz_ids
            )
        # Find nearest MAZ for each link, take min area type of i or j node
        for link in network.links():
            i_node, j_node = link.i_node, link.j_node
            a_maz = sp_index_maz.nearest(i_node.x, i_node.y)
            b_maz = sp_index_maz.nearest(j_node.x, j_node.y)
            link["@area_type"] = min(
                maz_landuse_data[a_maz]["area_type"],
                maz_landuse_data[b_maz]["area_type"],
            )

    def _load_maz_landuse_data(self) -> Dict[int, Dict[Any, Union[str, int, EmmeNode]]]:
        maz_data_file_path = self.get_abs_path(self.config.scenario.maz_landuse_file)
        maz_landuse_data = {}
        with open(maz_data_file_path, "r", encoding="utf8") as maz_data_file:
            header = [h.strip() for h in next(maz_data_file).split(",")]
            for line in maz_data_file:
                data = dict(zip(header, line.split(",")))
                maz_landuse_data[int(data["MAZ_ORIGINAL"])] = data
        return maz_landuse_data

    @staticmethod
    def _calc_maz_area_type(
        landuse_data: Dict[int, Dict[Any, Union[str, int, EmmeNode]]],
        maz_ids: List[int],
    ):
        # Sum total landuse attributes within buffer distance
        total_pop = sum(int(landuse_data[maz_id]["POP"]) for maz_id in maz_ids)
        total_emp = sum(int(landuse_data[maz_id]["emp_total"]) for maz_id in maz_ids)
        total_acres = sum(float(landuse_data[maz_id]["ACRES"]) for maz_id in maz_ids)
        # calculate buffer area type
        if total_acres > 0:
            density = (1 * total_pop + 2.5 * total_emp) / total_acres
        else:
            density = 0
        # code area type class
        if density < 6:
            return 5  # rural
        if density < 30:
            return 4  # suburban
        if density < 55:
            return 3  # urban
        if density < 100:
            return 2  # urban business
        if density < 300:
            return 1  # cbd
        return 0  # regional core

    @staticmethod
    def _set_capclass(network):
        for link in network.links():
            area_type = link["@area_type"]
            if area_type < 0:
                link["@capclass"] = -1
            else:
                link["@capclass"] = 10 * area_type + link["@ft"]

    def _set_speed(self, network):
        free_flow_speed_map = {}
        for row in self.config.highway.capclass_lookup:
            if row.get("free_flow_speed") is not None:
                free_flow_speed_map[row["capclass"]] = row.get("free_flow_speed")
        for link in network.links():
            # default speed of 25 mph if missing or 0 in table map
            link["@free_flow_speed"] = free_flow_speed_map.get(link["@capclass"], 25)
            speed = link["@free_flow_speed"] or 25
            link["@free_flow_time"] = 60 * link.length / speed
