"""Network Analysis and Documentation Tool

This script analyzes Emme network files and generates comprehensive documentation
including field definitions, value ranges, record counts, and comparisons to code expectations.

Usage:
    python scripts/network_analysis/analyze_network.py <network_path> [options]
    
Examples:
    # Analyze a single network scenario
    python scripts/network_analysis/analyze_network.py "M:/Development/Travel Model Two/Supply/Network Creation 2025/from_OSM/SanMateo/7_scenario/emme"
    
    # Compare to code expectations
    python scripts/network_analysis/analyze_network.py "path/to/network" --compare-expectations
    
    # Generate detailed report
    python scripts/network_analysis/analyze_network.py "path/to/network" --output report.md
"""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import json

# Add parent directory to path for tm2py imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    import inro.emme.desktop.app as _app
    import inro.emme.database.emmebank as _eb
    HAS_EMME = True
except (ImportError, ModuleNotFoundError):
    HAS_EMME = False
    print("WARNING: Emme not found. Running in documentation mode only.")

import pandas as pd
import numpy as np


class NetworkAnalyzer:
    """Analyzes Emme network files and generates documentation."""
    
    def __init__(self, network_path: Path):
        """Initialize analyzer with network path.
        
        Args:
            network_path: Path to Emme project or emmebank
        """
        self.network_path = Path(network_path)
        self.emmebank = None
        self.project = None
        
        if HAS_EMME:
            self._open_network()
    
    def _open_network(self):
        """Open the Emme network."""
        # Check for Database directories (Emme's actual database format)
        db_dirs = list(self.network_path.glob('Database*'))
        if db_dirs:
            # Use the first database directory found
            self.emmebank = _eb.Emmebank(str(db_dirs[0] / 'emmebank'))
        elif self.network_path.suffix == '.emp':
            # Direct .emp file path - find associated database
            db_dirs = list(self.network_path.parent.glob('Database*'))
            if db_dirs:
                self.emmebank = _eb.Emmebank(str(db_dirs[0] / 'emmebank'))
            else:
                raise ValueError(f"Cannot find Database directory for {self.network_path}")
        elif (self.network_path / 'emmebank').exists():
            # Direct emmebank directory
            self.emmebank = _eb.Emmebank(str(self.network_path / 'emmebank'))
        else:
            raise ValueError(f"Cannot find Emme network at {self.network_path}")
    
    def analyze_scenarios(self) -> Dict[str, Any]:
        """Analyze all scenarios in the emmebank.
        
        Returns:
            Dictionary with scenario information
        """
        if not HAS_EMME or not self.emmebank:
            return {"error": "Emme not available"}
        
        scenarios_info = []
        for scenario in self.emmebank.scenarios():
            info = {
                'id': scenario.id,
                'title': scenario.title,
                'has_traffic_results': scenario.has_traffic_results,
                'has_transit_results': scenario.has_transit_results,
                'num_nodes': len(list(scenario.get_partial_network(['NODE']).nodes())),
                'num_links': len(list(scenario.get_partial_network(['LINK']).links())),
            }
            
            # Get transit info if applicable
            network = scenario.get_partial_network(['TRANSIT_LINE'])
            if network:
                info['num_transit_lines'] = len(list(network.transit_lines()))
            
            scenarios_info.append(info)
        
        return {
            'count': len(scenarios_info),
            'scenarios': scenarios_info
        }
    
    def analyze_link_attributes(self, scenario_id: int) -> Dict[str, Any]:
        """Analyze link attributes for a scenario.
        
        Args:
            scenario_id: Scenario ID to analyze
            
        Returns:
            Dictionary with attribute analysis
        """
        if not HAS_EMME or not self.emmebank:
            return {"error": "Emme not available"}
        
        scenario = self.emmebank.scenario(scenario_id)
        if not scenario:
            return {"error": f"Scenario {scenario_id} not found"}
        
        network = scenario.get_network()
        
        # Get all link attributes
        link_attrs = network.attributes('LINK')
        
        analysis = {
            'scenario_id': scenario_id,
            'total_links': len(list(network.links())),
            'attributes': {}
        }
        
        for attr_name in link_attrs:
            attr_info = self._analyze_attribute(network, 'LINK', attr_name)
            analysis['attributes'][attr_name] = attr_info
        
        return analysis
    
    def analyze_node_attributes(self, scenario_id: int) -> Dict[str, Any]:
        """Analyze node attributes for a scenario.
        
        Args:
            scenario_id: Scenario ID to analyze
            
        Returns:
            Dictionary with attribute analysis
        """
        if not HAS_EMME or not self.emmebank:
            return {"error": "Emme not available"}
        
        scenario = self.emmebank.scenario(scenario_id)
        if not scenario:
            return {"error": f"Scenario {scenario_id} not found"}
        
        network = scenario.get_network()
        
        # Get all node attributes
        node_attrs = network.attributes('NODE')
        
        analysis = {
            'scenario_id': scenario_id,
            'total_nodes': len(list(network.nodes())),
            'attributes': {}
        }
        
        for attr_name in node_attrs:
            attr_info = self._analyze_attribute(network, 'NODE', attr_name)
            analysis['attributes'][attr_name] = attr_info
        
        return analysis
    
    def _analyze_attribute(self, network, domain: str, attr_name: str) -> Dict[str, Any]:
        """Analyze a specific attribute.
        
        Args:
            network: Emme network object
            domain: Domain (NODE, LINK, etc.)
            attr_name: Attribute name
            
        Returns:
            Dictionary with attribute statistics
        """
        values = []
        
        if domain == 'LINK':
            elements = network.links()
        elif domain == 'NODE':
            elements = network.nodes()
        elif domain == 'TRANSIT_LINE':
            elements = network.transit_lines()
        else:
            return {"error": f"Unknown domain: {domain}"}
        
        for element in elements:
            try:
                value = element[attr_name]
                if value is not None:
                    values.append(value)
            except:
                pass
        
        if not values:
            return {
                'type': 'unknown',
                'count': 0,
                'null_count': len(list(elements))
            }
        
        # Determine type
        sample_value = values[0]
        if isinstance(sample_value, (int, float, np.number)):
            return self._analyze_numeric_attribute(values, len(list(elements)))
        else:
            return self._analyze_categorical_attribute(values, len(list(elements)))
    
    def _analyze_numeric_attribute(self, values: List, total_count: int) -> Dict[str, Any]:
        """Analyze numeric attribute."""
        values = np.array(values, dtype=float)
        
        return {
            'type': 'numeric',
            'count': len(values),
            'null_count': total_count - len(values),
            'null_pct': (total_count - len(values)) / total_count * 100 if total_count > 0 else 0,
            'min': float(np.min(values)),
            'max': float(np.max(values)),
            'mean': float(np.mean(values)),
            'median': float(np.median(values)),
            'std': float(np.std(values)),
            'unique_values': int(len(np.unique(values))),
            'percentiles': {
                '10': float(np.percentile(values, 10)),
                '25': float(np.percentile(values, 25)),
                '75': float(np.percentile(values, 75)),
                '90': float(np.percentile(values, 90)),
            }
        }
    
    def _analyze_categorical_attribute(self, values: List, total_count: int) -> Dict[str, Any]:
        """Analyze categorical attribute."""
        value_counts = pd.Series(values).value_counts()
        
        return {
            'type': 'categorical',
            'count': len(values),
            'null_count': total_count - len(values),
            'null_pct': (total_count - len(values)) / total_count * 100 if total_count > 0 else 0,
            'unique_values': len(value_counts),
            'top_values': value_counts.head(10).to_dict(),
            'sample_values': list(value_counts.index[:5])
        }
    
    def compare_to_expectations(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Compare network attributes to code expectations.
        
        Args:
            analysis: Analysis results from analyze_link_attributes or analyze_node_attributes
            
        Returns:
            Dictionary with comparison results
        """
        # Load expected attributes from code
        expected_attrs = self._get_expected_attributes_from_code()
        
        actual_attrs = set(analysis.get('attributes', {}).keys())
        expected_set = set(expected_attrs.keys())
        
        comparison = {
            'missing_in_network': sorted(expected_set - actual_attrs),
            'unexpected_in_network': sorted(actual_attrs - expected_set),
            'attribute_comparisons': {}
        }
        
        # Compare attribute properties
        for attr_name in actual_attrs.intersection(expected_set):
            actual = analysis['attributes'][attr_name]
            expected = expected_attrs[attr_name]
            
            attr_comparison = {
                'matches': True,
                'issues': []
            }
            
            # Check type
            if actual.get('type') != expected.get('type'):
                attr_comparison['matches'] = False
                attr_comparison['issues'].append(
                    f"Type mismatch: expected {expected.get('type')}, found {actual.get('type')}"
                )
            
            # Check value ranges for numeric attributes
            if actual.get('type') == 'numeric' and 'range' in expected:
                exp_min, exp_max = expected['range']
                act_min, act_max = actual.get('min'), actual.get('max')
                
                if act_min < exp_min or act_max > exp_max:
                    attr_comparison['matches'] = False
                    attr_comparison['issues'].append(
                        f"Value range mismatch: expected [{exp_min}, {exp_max}], found [{act_min}, {act_max}]"
                    )
            
            comparison['attribute_comparisons'][attr_name] = attr_comparison
        
        return comparison
    
    def _get_expected_attributes_from_code(self) -> Dict[str, Dict[str, Any]]:
        """Extract expected attributes from tm2py code.
        
        Returns:
            Dictionary of expected attributes with their properties
        """
        # This would scan the codebase for attribute usage
        # For now, return known attributes from documentation
        expected = {
            # From create_tod_scenarios
            '@area_type': {'type': 'numeric', 'range': (0, 5), 'description': 'Area type classification'},
            '@capclass': {'type': 'numeric', 'description': 'Capacity class'},
            '@free_flow_speed': {'type': 'numeric', 'range': (0, 100), 'description': 'Free flow speed (mph)'},
            '@free_flow_time': {'type': 'numeric', 'range': (0, 9999), 'description': 'Free flow time (minutes)'},
            
            # From highway network
            '@capacity': {'type': 'numeric', 'description': 'Link capacity'},
            '@ft': {'type': 'numeric', 'range': (1, 99), 'description': 'Facility type'},
            '@lanes': {'type': 'numeric', 'range': (0, 20), 'description': 'Number of lanes'},
            
            # From toll processing
            '@bridgetoll_da': {'type': 'numeric', 'description': 'Bridge toll drive alone (cents)'},
            '@bridgetoll_sr2': {'type': 'numeric', 'description': 'Bridge toll shared ride 2 (cents)'},
            '@bridgetoll_sr3': {'type': 'numeric', 'description': 'Bridge toll shared ride 3+ (cents)'},
            
            # Link identifiers
            '#link_id': {'type': 'numeric', 'description': 'Link ID'},
            '#a_node': {'type': 'numeric', 'description': 'A-node ID'},
            '#b_node': {'type': 'numeric', 'description': 'B-node ID'},
            
            # Assignment results
            'volau': {'type': 'numeric', 'description': 'Auto volume'},
            'auto_time': {'type': 'numeric', 'description': 'Auto travel time (minutes)'},
        }
        
        return expected
    
    def generate_markdown_report(self, output_path: Path, **analyses):
        """Generate markdown documentation report.
        
        Args:
            output_path: Path to output markdown file
            **analyses: Named analysis results to include in report
        """
        lines = [
            "# Network Analysis Report",
            "",
            f"**Network Path**: `{self.network_path}`",
            "",
            f"**Generated**: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "---",
            ""
        ]
        
        # Scenarios summary
        if 'scenarios' in analyses:
            lines.extend(self._format_scenarios_section(analyses['scenarios']))
        
        # Link attributes
        if 'link_attrs' in analyses:
            lines.extend(self._format_attributes_section('Link', analyses['link_attrs']))
        
        # Node attributes
        if 'node_attrs' in analyses:
            lines.extend(self._format_attributes_section('Node', analyses['node_attrs']))
        
        # Comparison to expectations
        if 'comparison' in analyses:
            lines.extend(self._format_comparison_section(analyses['comparison']))
        
        # Write to file
        output_path.write_text('\n'.join(lines))
        print(f"Report written to: {output_path}")
    
    def _format_scenarios_section(self, scenarios: Dict[str, Any]) -> List[str]:
        """Format scenarios section."""
        lines = [
            "## Scenarios",
            "",
            f"**Total Scenarios**: {scenarios['count']}",
            "",
            "| ID | Title | Nodes | Links | Transit Lines | Traffic Results | Transit Results |",
            "|---|---|---|---|---|---|---|",
        ]
        
        for scenario in scenarios.get('scenarios', []):
            lines.append(
                f"| {scenario['id']} | {scenario['title']} | "
                f"{scenario.get('num_nodes', 'N/A'):,} | "
                f"{scenario.get('num_links', 'N/A'):,} | "
                f"{scenario.get('num_transit_lines', 'N/A')} | "
                f"{'✓' if scenario.get('has_traffic_results') else '✗'} | "
                f"{'✓' if scenario.get('has_transit_results') else '✗'} |"
            )
        
        lines.extend(["", "---", ""])
        return lines
    
    def _format_attributes_section(self, domain_name: str, analysis: Dict[str, Any]) -> List[str]:
        """Format attributes section."""
        lines = [
            f"## {domain_name} Attributes",
            "",
            f"**Scenario**: {analysis.get('scenario_id')}",
            f"**Total {domain_name}s**: {analysis.get(f'total_{domain_name.lower()}s', analysis.get('total_links', analysis.get('total_nodes', 0))):,}",
            "",
            f"**Total Attributes**: {len(analysis.get('attributes', {}))}",
            "",
        ]
        
        # Group attributes by type
        numeric_attrs = []
        categorical_attrs = []
        
        for attr_name, attr_info in sorted(analysis.get('attributes', {}).items()):
            if attr_info.get('type') == 'numeric':
                numeric_attrs.append((attr_name, attr_info))
            else:
                categorical_attrs.append((attr_name, attr_info))
        
        # Numeric attributes table
        if numeric_attrs:
            lines.extend([
                "### Numeric Attributes",
                "",
                "| Attribute | Count | Null % | Min | Max | Mean | Median | Std Dev | Unique |",
                "|---|---|---|---|---|---|---|---|---|",
            ])
            
            for attr_name, info in numeric_attrs:
                lines.append(
                    f"| `{attr_name}` | {info.get('count', 0):,} | "
                    f"{info.get('null_pct', 0):.1f}% | "
                    f"{info.get('min', 0):.2f} | "
                    f"{info.get('max', 0):.2f} | "
                    f"{info.get('mean', 0):.2f} | "
                    f"{info.get('median', 0):.2f} | "
                    f"{info.get('std', 0):.2f} | "
                    f"{info.get('unique_values', 0):,} |"
                )
            
            lines.extend(["", ""])
        
        # Categorical attributes table
        if categorical_attrs:
            lines.extend([
                "### Categorical Attributes",
                "",
                "| Attribute | Count | Null % | Unique Values | Top Values |",
                "|---|---|---|---|---|",
            ])
            
            for attr_name, info in categorical_attrs:
                top_values = info.get('top_values', {})
                top_str = ', '.join(f"{k} ({v})" for k, v in list(top_values.items())[:3])
                
                lines.append(
                    f"| `{attr_name}` | {info.get('count', 0):,} | "
                    f"{info.get('null_pct', 0):.1f}% | "
                    f"{info.get('unique_values', 0):,} | "
                    f"{top_str} |"
                )
            
            lines.extend(["", ""])
        
        lines.extend(["---", ""])
        return lines
    
    def _format_comparison_section(self, comparison: Dict[str, Any]) -> List[str]:
        """Format comparison to expectations section."""
        lines = [
            "## Comparison to Code Expectations",
            "",
        ]
        
        # Missing attributes
        missing = comparison.get('missing_in_network', [])
        if missing:
            lines.extend([
                "### ⚠️ Attributes Expected by Code but Missing in Network",
                "",
            ])
            for attr in missing:
                lines.append(f"- `{attr}`")
            lines.extend(["", ""])
        else:
            lines.extend([
                "### ✓ All Expected Attributes Present",
                "",
            ])
        
        # Unexpected attributes
        unexpected = comparison.get('unexpected_in_network', [])
        if unexpected:
            lines.extend([
                "### Additional Attributes in Network (Not Expected by Code)",
                "",
            ])
            for attr in unexpected[:20]:  # Limit to first 20
                lines.append(f"- `{attr}`")
            if len(unexpected) > 20:
                lines.append(f"- ... and {len(unexpected) - 20} more")
            lines.extend(["", ""])
        
        # Attribute mismatches
        mismatches = {k: v for k, v in comparison.get('attribute_comparisons', {}).items() 
                     if not v.get('matches', True)}
        
        if mismatches:
            lines.extend([
                "### Attribute Validation Issues",
                "",
                "| Attribute | Issues |",
                "|---|---|",
            ])
            
            for attr_name, info in sorted(mismatches.items()):
                issues_str = '; '.join(info.get('issues', []))
                lines.append(f"| `{attr_name}` | {issues_str} |")
            
            lines.extend(["", ""])
        else:
            lines.extend([
                "### ✓ All Attribute Validations Passed",
                "",
            ])
        
        lines.extend(["---", ""])
        return lines


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Analyze Emme network and generate documentation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        'network_path',
        type=Path,
        help='Path to Emme project or emmebank directory'
    )
    
    parser.add_argument(
        '--scenario',
        type=int,
        default=None,
        help='Scenario ID to analyze (if not specified, analyzes all scenarios)'
    )
    
    parser.add_argument(
        '--output',
        '-o',
        type=Path,
        default=None,
        help='Output markdown file path (default: network_analysis.md)'
    )
    
    parser.add_argument(
        '--compare-expectations',
        action='store_true',
        help='Compare network attributes to code expectations'
    )
    
    parser.add_argument(
        '--json',
        type=Path,
        default=None,
        help='Also output raw analysis as JSON'
    )
    
    args = parser.parse_args()
    
    if not HAS_EMME:
        print("ERROR: Emme is required to analyze networks")
        print("Install Emme or run this script in an Emme Python environment")
        return 1
    
    # Create analyzer
    print(f"Analyzing network at: {args.network_path}")
    analyzer = NetworkAnalyzer(args.network_path)
    
    # Analyze scenarios
    print("Analyzing scenarios...")
    scenarios = analyzer.analyze_scenarios()
    
    # Determine which scenario to analyze in detail
    if args.scenario:
        scenario_ids = [args.scenario]
    elif scenarios['count'] > 0:
        # Use first scenario by default
        scenario_ids = [scenarios['scenarios'][0]['id']]
    else:
        print("ERROR: No scenarios found in network")
        return 1
    
    # Analyze attributes
    analyses = {'scenarios': scenarios}
    
    for scenario_id in scenario_ids:
        print(f"Analyzing scenario {scenario_id}...")
        
        print("  - Link attributes...")
        link_analysis = analyzer.analyze_link_attributes(scenario_id)
        analyses['link_attrs'] = link_analysis
        
        print("  - Node attributes...")
        node_analysis = analyzer.analyze_node_attributes(scenario_id)
        analyses['node_attrs'] = node_analysis
        
        if args.compare_expectations:
            print("  - Comparing to code expectations...")
            comparison = analyzer.compare_to_expectations(link_analysis)
            analyses['comparison'] = comparison
    
    # Generate outputs
    if args.json:
        print(f"Writing JSON to: {args.json}")
        with open(args.json, 'w') as f:
            json.dump(analyses, f, indent=2)
    
    output_path = args.output or Path('network_analysis.md')
    print(f"Generating markdown report...")
    analyzer.generate_markdown_report(output_path, **analyses)
    
    print("\n✓ Analysis complete!")
    return 0


if __name__ == '__main__':
    sys.exit(main())
