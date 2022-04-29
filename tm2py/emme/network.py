"""Module for Emme network manipulations.
"""

from collections import defaultdict as _defaultdict
from itertools import product as _product
import heapq
from math import sqrt, ceil
import os
from typing import Any, Callable, Union, Dict, List, Tuple

from tm2py.emme.manager import EmmeScenario, EmmeManager, EmmeNode, EmmeLink
EmmeNetworkCalcSpecification = Dict[str, Union[str, Dict[str, str]]]


class NetworkCalculator:
    """Simple wrapper interface to the Emme Network calculator

    Used to generate the standard network calculator specification (dictionary)
    from argument inputs. Useful when NOT (commonly) using selection or
    aggregation options, and mostly running link expression calculations

    Args:
        scenario: Emme scenario object
    """

    def __init__(self, scenario: EmmeScenario):
        self._scenario = scenario
        emme_manager = _manager.EmmeManager()
        modeller = emme_manager.modeller()
        self._network_calc = modeller.tool(
            "inro.emme.network_calculation.network_calculator"
        )
        self._specs = []

    def __call__(
        self,
        result: str,
        expression: str,
        selections: Union[str, Dict[str, str]] = None,
        aggregation: Dict[str, str] = None,
    ) -> Dict[str, float]:
        """Run a network calculation in the scenario, see the Emme help for more.

        Args:
            result: Name of network attribute
            expression: Calculation expression
            selections: Selection expression nest. Defaults to {"link": "all"} if
                        not specified, and is used as a link selection expression
                        if specified as a string.
            aggregation: Aggregation operators if aggregating between network domains.

        Returns:
            A dictionary report with min, max, average and sum of the calculation
            expression. See Emme help 'Network calculator' for more.
        """
        spec = self._format_spec(result, expression, selections, aggregation)
        return self._network_calc(spec, self._scenario)

    def add_calc(
        self,
        result: str,
        expression: str,
        selections: Union[str, Dict[str, str]] = None,
        aggregation: Dict[str, str] = None,
    ):
        """Add calculation to list of network calculations to run.

        Args:
            result: Name of network attribute
            expression: Calculation expression
            selections: Selection expression nest. Defaults to {"link": "all"} if
                        not specified, and is used as a link selection expression
                        if specified as a string.
            aggregation: Aggregation operators if aggregating between network domains.
        """
        self._specs.append(
            self._format_spec(result, expression, selections, aggregation)
        )

    def run(self) -> List[Dict[str, float]]:
        """Run accumulated network calculations all at once.

        Returns:
            A list of dictionary reports with min, max, average and sum of the
            calculation expression. See Emme help 'Network calculator' for more.
        """
        reports = self._network_calc(self._specs, self._scenario)
        self._specs = []
        return reports

    @staticmethod
    def _format_spec(
        result: str,
        expression: str,
        selections: Union[str, Dict[str, str]],
        aggregation: Dict[str, str],
    ) -> EmmeNetworkCalcSpecification:
        spec = {
            "result": result,
            "expression": expression,
            "aggregation": aggregation,
            "type": "NETWORK_CALCULATION",
        }
        if selections is not None:
            if isinstance(selections, str):
                selections = {"link": selections}
            spec["selections"] = selections
        else:
            spec["selections"] = {"link": "all"}
        return spec


def copy_tod_scenario(
    ref_scenario: EmmeScenario,
    time_period_name: str,
    scenario_id: int,
    all_period_names: List[str],
):
    """Copy the reference scenario setting the named per-period attributes.

    The network data is duplicated from the base ref_scenario, and the
    attributes for this time period only are copied to the root name,
    e.g. @lanes_am -> @lanes. The attributes for the other time
    periods are deleted.

    Args:
        ref_scenario: base "all-time period" scenario
        time_period_name: name used for this time period
        scenario_id: scenario_id used for this time period
        all_period_names: list of all time period names
    """
    # Copy ref scenario
    emmebank = ref_scenario.emmebank
    scenario = emmebank.scenario(scenario_id)
    if scenario:
        emmebank.delete_scenario(scenario)
    scenario = emmebank.copy_scenario(ref_scenario, scenario_id)
    scenario.title = f"{time_period_name} {ref_scenario.title}"[:60]
    # find all time-of-day attributes (ends with period name), and
    # create attributes without period suffix, copy values for this period
    # and delete all other period attributes
    end = -len(time_period_name)
    all_period_names = [name.lower() for name in all_period_names]
    for attr in scenario.extra_attributes():
        if attr.name.endswith(time_period_name.lower()):
            root_attr = attr.name[:end]
            if root_attr.endswith("_"):
                root_attr = root_attr[:-1]
            new_attr = scenario.create_extra_attribute(attr.type, root_attr)
            new_attr.description = attr.description
            values = scenario.get_attribute_values(attr.type, [attr.name])
            scenario.set_attribute_values(attr.type, [root_attr], values)
        for name in all_period_names:
            if attr.name.endswith(name):
                scenario.delete_extra_attribute(attr)


_CRS_WKT = """PROJCS["NAD83(HARN) / California zone 6 (ftUS)",GEOGCS["NAD83(HARN)",
DATUM["NAD83_High_Accuracy_Reference_Network",SPHEROID["GRS 1980",6378137,298.257222101,
AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6152"]],PRIMEM["Greenwich",
0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],
AUTHORITY["EPSG","4152"]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER[
"standard_parallel_1",33.88333333333333],PARAMETER["standard_parallel_2",32.78333333333333],
PARAMETER["latitude_of_origin",32.16666666666666],PARAMETER["central_meridian",-116.25],
PARAMETER["false_easting",6561666.667],PARAMETER["false_northing",1640416.667],UNIT[
"US survey foot",0.3048006096012192,AUTHORITY["EPSG","9003"]],AXIS["X",EAST],AXIS["Y",NORTH],
AUTHORITY["EPSG","2875"]]"""


def project_coordinates(
    ref_scenario: EmmeScenario, emme_manager: EmmeManager, project_path: str
):
    """Project network coordinates to NAD83(HARN) California zone 6 (ftUS).

    Args:
        ref_scenario: the Emme scenario to reproject
        emme_manager: EmmeManager object
        project_path: path to Emme project (.emp file)
    """
    modeller = emme_manager.modeller
    project_coord = modeller.tool(
        "inro.emme.data.network.base.project_network_coordinates"
    )
    project_root = os.path.dirname(project_path)
    emme_app = emme_manager.project(project_path)
    src_prj_file = emme_app.project.spatial_reference_file
    if not src_prj_file:
        raise Exception(
            "Emme network coordinate reference system is not specified, "
            "unable to project coordinates for area type calculation. "
            "Set correct Spatial Reference in Emme Project settings -> GIS."
        )
    with open(src_prj_file, "r", encoding="utf8") as src_prj:
        current_wkt = src_prj.read()
    crs_wkt = _CRS_WKT.replace("\n", "")
    if current_wkt != crs_wkt:
        dst_prj_file = os.path.join(
            project_root, "Media", "NAD83(HARN) California zone 6 (ftUS).prj"
        )
        with open(dst_prj_file, "w", encoding="utf8") as dst_prj:
            dst_prj.write(crs_wkt)
        project_coord(
            from_scenario=ref_scenario,
            from_proj_file=src_prj_file,
            to_proj_file=dst_prj_file,
            overwrite=True,
        )
        emme_app.project.spatial_reference.file_path = dst_prj_file
        emme_app.project.save()


class IDGenerator:
    """Generate available Node IDs."""

    def __init__(self, start, network):
        """Return new Emme network attribute with details as defined."""
        self._number = start
        self._network = network

    def next(self):
        """Return the next valid node ID number."""
        while True:
            if self._network.node(self._number) is None:
                break
            self._number += 1
        return self._number

    def __next__(self):
        """Return the next valid node ID number."""
        return self.next()


_INF = 1e400


def find_path(
    orig_node: EmmeNode, dest_node: EmmeNode, filter_func: Callable, cost_func: Callable
) -> List[EmmeLink]:
    """Find and return the shortest path (sequence of links) between two nodes in Emme network.

    Args:
        orig_node: origin Emme node object
        dest_node: destination Emme node object
        filter_func: callable function which accepts an Emme network link and returns
            True if included and False if excluded. E.g. lambda link: mode in link.modes
        cost_func: callable function which accepts an Emme network link and returns the
            cost value for the link.
    """
    # pylint: disable=R0912
    # disable too many branches recommendation
    visited = set([])
    costs = _defaultdict(lambda: _INF)
    back_links = {}
    heap = []
    outgoing = None
    link_found = False
    for outgoing in orig_node.outgoing_links():
        if filter_func(outgoing):
            back_links[outgoing] = None
            if outgoing.j_node == dest_node:
                link_found = True
                break
            cost_to_link = cost_func(outgoing)
            costs[outgoing] = cost_to_link
            heapq.heappush(heap, (cost_to_link, outgoing))
    try:
        while not link_found:
            cost_to_link, link = heapq.heappop(heap)
            if link in visited:
                continue
            visited.add(link)
            for outgoing in link.j_node.outgoing_links():
                if not filter_func(outgoing):
                    continue
                if outgoing in visited:
                    continue
                outgoing_cost = cost_to_link + cost_func(outgoing)
                if outgoing_cost < costs[outgoing]:
                    back_links[outgoing] = link
                    costs[outgoing] = outgoing_cost
                    heapq.heappush(heap, (outgoing_cost, outgoing))
                if outgoing.j_node == dest_node:
                    link_found = True
                    break
    except IndexError:
        pass  # IndexError if heap is empty
    if not link_found or outgoing is None:
        raise NoPathFound(f"No path found between {orig_node} and {dest_node}")
    prev_link = outgoing
    route = []
    while prev_link:
        route.append(prev_link)
        prev_link = back_links[prev_link]
    return list(reversed(route))


class NoPathFound(Exception):
    """No path found error"""


class SpatialGridIndex:
    """Simple spatial grid hash for fast (enough) spatial search.

    Support nearest neighbor / within distance searches of points.
    Input points are indexed to the grid, supporting fast O(1) indexing
    of relative positions.

    Args:
        size: the size of the grid to use for the index, relative to the point coordinates.
            Optimal size will depend upon the dataset and the nature of the queries, but
            a good starting point is half the median query distance.

    Internal properties:
        _size: input grid storage size
        _grid_index: the indexed grid, a dictionary of the grid squares
            by their lower right corner to objects with that square.
    """

    def __init__(self, size: float):
        self._size = float(size)
        self._grid_index = _defaultdict(lambda: [])

    def insert(self, obj: Any, x_coord: float, y_coord: float):
        """Add new obj with coordinates x_coord and y_coord.

        Args:
           obj: any python object, will be returned from search methods
           x_coord: x-coordinate
           y_coord: y-coordinate
        """
        grid_x, grid_y = round(x_coord / self._size), round(y_coord / self._size)
        self._grid_index[(grid_x, grid_y)].append((obj, x_coord, y_coord))

    def nearest(self, x_coord: float, y_coord: float):
        """Return the closest object in index to the specified coordinates

        Args:
            x_coord: x-coordinate
            y_coord: y-coordinate
        """
        if len(self._grid_index) == 0:
            raise Exception("SpatialGrid is empty.")
        grid_x, grid_y = round(x_coord / self._size), round(y_coord / self._size)
        # find all items in a search pattern around this items grid,
        # continuing fanning out until at least one item is found, complete each
		# increment of grids in case a closer item is found in next tile
        step = 0
        done = False
        found_items = []
        while not done:
            search_offsets = list(range(-1 * step, step + 1))
            search_offsets = _product(search_offsets, search_offsets)
            items = []
            for x_offset, y_offset in search_offsets:
                if abs(x_offset) != step and abs(y_offset) != step:
                    continue  # already checked this grid tile
                items.extend(self._grid_index[grid_x + x_offset, grid_y + y_offset])
            if found_items:
                done = True
            found_items.extend(items)
            step += 1

        return self._closest_candidate(x_coord, y_coord, found_items)

    @staticmethod
    def _closest_candidate(
        x_coord: float, y_coord: float, candidate_items: List[Tuple[float, float, Any]]
    ) -> Any:
        """Calculated actual distance from point coordinates to found_items, and return closest.

        Args:
            x_coord: x-coordinate
            y_coord: y-coordinate
            candidate_items: List of tuples of object, x_coord, y_coord of candidate items
        """

        def calc_dist(x_1, y_1, x_2, y_2):
            return sqrt((x_1 - x_2) ** 2 + (y_1 - y_2) ** 2)

        min_dist = 1e400
        closest = None
        for obj, x_coord_2, y_coord_2 in candidate_items:
            dist = calc_dist(x_coord, y_coord, x_coord_2, y_coord_2)
            if dist < min_dist:
                closest = obj
                min_dist = dist
        return closest

    def within_distance(self, x_coord: float, y_coord: float, distance: float):
        """Return all objects in index within the distance of the specified coordinates

        Args:
            x_coord: x-coordinate
            y_coord: y-coordinate
            distance: distance to search in point coordinate units
        """

        def point_in_circle(x_1, y_1, x_2, y_2, dist):
            return sqrt((x_1 - x_2) ** 2 + (y_1 - y_2) ** 2) <= dist

        return self._get_items_on_grid(x_coord, y_coord, distance, point_in_circle)

    def within_square(self, x_coord: float, y_coord: float, distance: float):
        """Return all objects in index within a square box distance of the specified coordinates.

        Args:
            x_coord: x-coordinate
            y_coord: y-coordinate
            distance: distance to search in point coordinate units
        """

        def point_in_box(x_1, y_1, x_2, y_2, dist):
            return abs(x_1 - x_2) <= dist and abs(y_1 - y_2) <= dist

        return self._get_items_on_grid(x_coord, y_coord, distance, point_in_box)

    def _get_items_on_grid(
        self, x_coord: float, y_coord: float, distance: float, filter_func: Callable
    ):
        """Return all objects in index within distance of the specified coordinates.

        Args:
            x_coord: x-coordinate
            y_coord: y-coordinate
            distance: distance to search in point coordinate units
            filter_func: callable which determines if two points are within dist
                signature: (x_1, y1, x2, y2, dist)
        """
        grid_x, grid_y = round(x_coord / self._size), round(y_coord / self._size)
        num_search_grids = ceil(distance / self._size)
        search_offsets = list(range(-1 * num_search_grids, num_search_grids + 1))
        search_offsets = list(_product(search_offsets, search_offsets))
        items = []
        for x_offset, y_offset in search_offsets:
            items.extend(self._grid_index[grid_x + x_offset, grid_y + y_offset])
        filtered_items = [
            i for i, xi, yi in items if filter_func(x_coord, y_coord, xi, yi, distance)
        ]
        return filtered_items
