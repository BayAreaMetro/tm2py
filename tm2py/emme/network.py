from typing import Union, Dict


import tm2py.emme.manager as _manager

# PyLint cannot build AST from compiled Emme libraries
# so disabling relevant import module checks
# pylint: disable=E0611, E0401, E1101
from inro.emme.database.scenario import Scenario as EmmeScenario


class NetworkCalculator:
    """Simple wrapper interface to the Emme Network calculator

    Used to generate the standard network calculator specification (dictionary)
    from argument inputs. Useful when NOT (commonly) using selection or
    aggregation options, and mostly running link expression calculations
    """

    def __init__(self, scenario: EmmeScenario):
        """

        Args:
            scenario:
        """
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
    ):
        """Run a network calculation in the scenario, see the Emme help for more.

        Args:
            result: Name of network attribute
            expression: Calculation expression
            selections: Selection expression nest. Defaults to {"link": "all"} if
                        not specified, and is used as a link selection expression
                        if specified as a string.
            aggregation: Aggregation operators if aggregating between network domains.
        """
        spec = self._format_spec(result, expression, selections, aggregation)
        return self._network_calc(spec, self._scenario)

    def add_calc(
        self,
        result: str,
        expression: str,
        selections: Union[str, dict] = None,
        aggregation: [dict] = None,
    ):
        """Add calculation to list of network calclations to run.

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

    def run(self):
        """Run accumulated network calculations all at once."""
        reports = self._network_calc(self._specs, self._scenario)
        self._specs = []
        return reports

    @staticmethod
    def _format_spec(result, expression, selections, aggregation):
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
