"""Assigns and calculated skims for walk and bike at MAZ, TAP or TAZ.

 Compute zone-to-zone walk distance and bicycle distances:

     pedestrian maz -> maz
     pedestrian maz -> tap
     bicycle maz -> maz
     bicycle maz -> tap
     bicycle taz -> taz
     pedestrian tap -> tap

See \\model-files\\scripts\\preprocess\\CreateNonMotorizedNetwork.job 
and \\model-files\\scripts\\skims\\NonMotorizedSkims.job

TODO: docstring details

    TODO: values to come from config
        ;maximum pedestrian distance is 3 miles = 3*5280
        max_ped_distance = 15840
        ;maximum short bike distance is 3 miles = 3*5280
        max_bike_short_distance = 15840
        ;to effectively set no maximum, use ~300 mile bike distance
        nomax_bike_distance = 1500000
        ;max_drive_gencost ~= 5 miles @ 40 mph = 11
        ;                   = time + (0.6 / vot) * (dist * opcost) 
        ;                   = 5 / 40 * 60 + (0.6 / 15.0) * (5 * 17.90)
        max_drive_gencost = 11
        ;maximum pedestrian tap-tap distance is 1/2 miles = 0.5*5280
        max_tap_ped_distance = 2640

 Input:  A scenario network containing the fields 
         FT (functional type), 
         BIKEPEDOK, 
         CNTYPE (link type), and 
         ONEWAY

 Output: (1) Shortest path skims in csv format: from_zone,to_zone,shortest_path_cost,skim_value
             (note that the to_zone is printed twice)
         ped_distance_maz_maz.txt
"""

import array as _array

# from collections import defaultdict as _defaultdict
from contextlib import contextmanager as _context
import os as _os

# from typing import List


from tm2py.core.component import Component as _Component, Controller as _Controller
import tm2py.core.emme as _emme_tools


_join, _dir = _os.path.join, _os.path.dirname
EmmeScenario = _emme_tools.EmmeScenario


class ActiveModesAssign(_Component):
    """MAZ-to-MAZ 
    """

    def __init__(self, controller: _Controller, scenario: EmmeScenario):
        """TODO.

        Args:
            controller: parent Controller object
            scenario: Emme scenario
        """
        super().__init__(controller)
        self._scenario = scenario
        self._modeller = _emme_tools.EmmeProjectCache().modeller

        # self._net_calc = _emme_tools.NetworkCalculator(self._scenario, self._modeller)

        # Internal attributes to track data through the sequence of steps
        # self._mazs = None
        # self._demand = None
        # self._max_dist = 0
        # self._network = None
        # self._root_index = None
        # self._leaf_index = None

    def run(self):
        """Run """
        period = self._period

        counties = [
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
        # TODO: filter walk access or bike access links ?
        # TODO: output files ? append results by county to file ?
        skim_types = [
            {
                "roots": "MAZ",
                "leafs": "MAZ",
                "max_dist": max_ped_distance,
                "output": "",
            },  # walk, MAZ<->MAZ
            {
                "roots": "MAZ",
                "leafs": "TAP",
                "max_dist": max_ped_distance,
                "output": "",
            },  # walk, MAZ ->TAP
            {
                "roots": "MAZ",
                "leafs": "MAZ",
                "max_dist": max_bike_short_distance,
                "output": "",
            },  # bike, MAZ<->MAZ
            {
                "roots": "MAZ",
                "leafs": "TAP",
                "max_dist": max_bike_short_distance,
                "output": "",
            },  # bike, MAZ ->TAP
            {
                "roots": "TAZ",
                "leafs": "TAZ",
                "max_dist": max_bike_short_distance,
                "output": "",
            },  # bike, TAZ<->TAZ
            {
                "roots": "TAP",
                "leafs": "TAP",
                "max_dist": max_tap_ped_distance,
                "output": "",
            },  # walk, TAP<->TAP
        ]
        with self._setup():
            # calculate 6 skim types X 9 counties
            # (splitting by county should limit RAM consumption
            #  and might help runtime a little, to be tested)
            for skim in skim_types:
                for county in counties:
                    self._prepare_network(skim, county)
                    self._run_shortest_path(skim["max_dist"])

    @_context
    def _setup(self):
        # may want to create separate scenarios ?
        # Or use temp scenario
        # create @root and @leaf attributes,
        try:
            yield
        finally:
            # delete @root and @leaf attributes,
            #
            pass

    def _prepare_network(self):
        # Set mode access on network bike or walk
        #   For pedestrians
        #        (1) Take all of the one-way links in the network,
        #            and add the opposite direction for pedestrians.
        #        (2) Drop all non-pedestrian links. This means include
        #             all non-highway road links and pedestrian paths.
        #   For bicycle, all non-highway road links as well as bike
        #   paths/trails are retained.
        #   WALK_ACCESS = @walk_access
        #   BIKE_ACCESS = @bike_access
        #   CNTYPE -> determined from node "@tazseq", "@mazseq", "@tapseq"
        # Remove non-active connectors
        # Set origin by county and all destinations, MAZ, TAZ or TAP
        pass

    def _run_shortest_path(self, max_dist):
        # TODO: output:
        #    return numpy array and write to CSV
        shortest_paths_tool = self._modeller.tool(
            "inro.emme.network_calculation.shortest_path"
        )
        root_dir = _dir(self._scenario.emmebank.path)
        num_processors = self.config.emme.number_of_processors
        shortest_paths_tool(
            # modes=?,
            roots_attribute="@root",
            leafs_attribute="@leaf",
            link_cost_attribute="length",
            num_processors=num_processors,
            direction="FORWARD",
            # paths_file=_join(root_dir, f"shortest_paths_.ebp"),
            export_format_paths="BINARY",
            through_leaves=False,
            through_centroids=False,
            # max_radius=max_radius,
            max_cost=max_dist,
        )
