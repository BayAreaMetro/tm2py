"""
"""

from collections import defaultdict as _defaultdict
import os

from tm2py.core.component import Component as _Component, Controller as _Controller
import tm2py.core.emme as _emme_tools
from tm2py.core.logging import LogStartEnd

_crs_wkt = '''PROJCS["NAD83(HARN) / California zone 6 (ftUS)",GEOGCS["NAD83(HARN)",
DATUM["NAD83_High_Accuracy_Reference_Network",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],
TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6152"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",
0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4152"]],PROJECTION["Lambert_Conformal_Conic_2SP"],
PARAMETER["standard_parallel_1",33.88333333333333],PARAMETER["standard_parallel_2",32.78333333333333],
PARAMETER["latitude_of_origin",32.16666666666666],PARAMETER["central_meridian",-116.25],PARAMETER["false_easting",
6561666.667],PARAMETER["false_northing",1640416.667],UNIT["US survey foot",0.3048006096012192,AUTHORITY["EPSG",
"9003"]],AXIS["X",EAST],AXIS["Y",NORTH],AUTHORITY["EPSG","2875"]] '''


class CreateTODScenarios(_Component):
    """Highway assignment and skims"""

    def __init__(self, controller: _Controller):
        """Highway assignment and skims.

        Args:
            controller: parent Controller object
        """
        super().__init__(controller)
        self._emme_manager = None

    @LogStartEnd("Create 5 highway time of day scenarios.")
    def run(self):
        project_path = os.path.join(self.root_dir, self.config.emme.project_path)
        self._emme_manager = _emme_tools.EmmeManager()
        emme_app = self._emme_manager.project(project_path)
        self._emme_manager.init_modeller(emme_app)
        self._create_highway_scenarios()
        self._create_transit_scenarios()

    def _project_coordinates(self, ref_scenario):
        modeller = self._emme_manager.modeller
        project_coord = modeller.tool(
            "inro.emme.data.network.base.project_network_coordinates")

        project_path = os.path.join(self.root_dir, self.config.emme.project_path)
        project_root = os.path.dirname(project_path)
        emme_app = self._emme_manager.project(project_path)
        src_prj_file = emme_app.project.spatial_reference_file
        if not src_prj_file:
            raise Exception(
                "Emme network coordinate reference system is not specified, unable to project coordinates for "
                "area type calculation. Set correct Spatial Reference in Emme Project settings -> GIS."
            )
        with open(src_prj_file, 'r') as src_prj:
            current_wkt = src_prj.read()
        if current_wkt != _crs_wkt:
            dst_prj_file = os.path.join(project_root, "Media", "NAD83(HARN) California zone 6 (ftUS).prj")
            with open(dst_prj_file, 'w') as dst_prj:
                dst_prj.write(_crs_wkt)
            project_coord(from_scenario=ref_scenario,
                          from_proj_file=src_prj_file,
                          to_proj_file=dst_prj_file,
                          overwrite=True)
            emme_app.project.spatial_reference.file_path = dst_prj_file
            emme_app.project.save()

    def _create_highway_scenarios(self):
        emmebank_path = os.path.join(self.root_dir, self.config.emme.highway_database_path)
        emmebank = self._emme_manager.emmebank(emmebank_path)
        # create VDFs & set cross-reference function parameters
        emmebank.extra_function_parameters.el1 = "@free_flow_time"
        emmebank.extra_function_parameters.el2 = "@capacity"
        emmebank.extra_function_parameters.el3 = "@ja"
        # TODO: should have just 3 functions, and map the FT to the vdf
        # TODO: could optimize expression (to review)
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
        for f_id in ["fd3", "fd4", "fd5", "fd7", "fd8", "fd10", "fd11", "fd12", "fd13", "fd14"]:
            if emmebank.function(f_id):
                emmebank.delete_function(f_id)
            emmebank.create_function(f_id, akcelik_tmplt)
        if emmebank.function("fd6"):
            emmebank.delete_function("fd6")
        emmebank.create_function("fd6", fixed_tmplt)
        self._emme_manager.change_emmebank_dimensions(emmebank, {"full_matrices": 9999})

        ref_scenario = emmebank.scenario(self.config.emme.all_day_scenario_id)
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
            for period in self.config.periods:
                if attr.name.endswith(period.name):
                    tod_attr_groups[attr.type][attr.name[:-len(period.name)]].append(attr.name)
        for period in self.config.periods:
            scenario = emmebank.scenario(period.emme_scenario_id)
            if scenario:
                emmebank.delete_scenario(scenario)
            scenario = emmebank.copy_scenario(ref_scenario, period.emme_scenario_id)
            scenario.title = f"{period.name} {ref_scenario.title}"[:60]
            # in per-period scenario create attributes without period suffix, copy values 
            # for this period and delete all other period attributes
            for domain, all_attrs in tod_attr_groups.items():
                for root_attr, tod_attrs in all_attrs.items():
                    src_attr = f"{root_attr}{period.name}"
                    if root_attr.endswith("_"):
                        root_attr = root_attr[:-1]
                    for attr in tod_attrs:
                        if attr != src_attr:
                            scenario.delete_extra_attribute(attr)
                    attr = scenario.create_extra_attribute(domain, root_attr)
                    attr.description = scenario.extra_attribute(src_attr).description
                    values = scenario.get_attribute_values(domain, [src_attr])
                    scenario.set_attribute_values(domain, [root_attr], values)
                    scenario.delete_extra_attribute(src_attr)

    def _create_transit_scenarios(self):
        emmebank_path = os.path.join(self.root_dir, self.config.emme.transit_database_path)
        emmebank = self._emme_manager.emmebank(emmebank_path)
        self._emme_manager.change_emmebank_dimensions(emmebank, {"full_matrices": 9999})