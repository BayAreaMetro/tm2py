"""Module for Emme network calculations.

Contains NetworkCalculator class to generate Emme format specifications for
the Network calculator.
"""
import heapq
from collections import defaultdict as _defaultdict
from typing import Any, Callable, Dict, List, Union

from inro.emme.network.link import Link as EmmeNetworkLink
from inro.emme.network.node import Node as EmmeNetworkNode

import tm2py.emme.manager as _manager

EmmeScenario = _manager.EmmeScenario
EmmeNetworkCalcSpecification = Dict[str, Union[str, Dict[str, str]]]

_INF = 1e400


class NetworkCalculator:
    """Simple wrapper interface to the Emme Network calculator.

    Used to generate the standard network calculator specification (dictionary)
    from argument inputs. Useful when NOT (commonly) using selection or
    aggregation options, and mostly running link expression calculations

    Args:
        scenario: Emme scenario object
    """

    def __init__(self, controller, scenario: EmmeScenario):
        """Constructor for NetworkCalculator class.

        Args:
            scenario (EmmeScenario): Reference EmmeScenario object
        """
        self._scenario = scenario
        self._network_calc = controller.emme_manager.modeller.tool(
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

        Returns
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


def find_path(
    orig_node: EmmeNetworkNode,
    dest_node: EmmeNetworkNode,
    filter_func: Callable,
    cost_func: Callable,
) -> List[EmmeNetworkLink]:
    """Find and return the shortest path (sequence of links) between two nodes in Emme network.
    Args:
        orig_node: origin Emme node object
        dest_node: desination Emme node object
        filter_func: callable function which accepts an Emme network link and returns True if included and False
            if excluded. E.g. lambda link: mode in link.modes
        cost_func: callable function which accepts an Emme network link and returns the cost value for the link.
    """
    visited = set([])
    visited_add = visited.add
    costs = _defaultdict(lambda: _INF)
    back_links = {}
    heap = []
    pop, push = heapq.heappop, heapq.heappush
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
            push(heap, (cost_to_link, outgoing))
    try:
        while not link_found:
            cost_to_link, link = pop(heap)
            if link in visited:
                continue
            visited_add(link)
            for outgoing in link.j_node.outgoing_links():
                if not filter_func(outgoing):
                    continue
                if outgoing in visited:
                    continue
                outgoing_cost = cost_to_link + cost_func(outgoing)
                if outgoing_cost < costs[outgoing]:
                    back_links[outgoing] = link
                    costs[outgoing] = outgoing_cost
                    push(heap, (outgoing_cost, outgoing))
                if outgoing.j_node == dest_node:
                    link_found = True
                    break
    except IndexError:
        pass  # IndexError if heap is empty
    if not link_found or outgoing is None:
        raise NoPathFound("No path found between %s and %s" % (orig_node, dest_node))
    prev_link = outgoing
    route = []
    while prev_link:
        route.append(prev_link)
        prev_link = back_links[prev_link]
    return list(reversed(route))


class NoPathFound(Exception):
    pass
