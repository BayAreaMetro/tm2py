#!/usr/bin/env python3
"""
TM2PY Network Summary Script

This script generates comprehensive network performance summaries across all time periods
including VMT, VHT, and delay calculations by facility type and user class.

Outputs:
    - VMT/VHT/Delay by facility classification
    - Performance metrics by user class  
    - County-level summaries
    - Lane mile inventories

Usage:
    python network_summary.py

Requirements:
    - EMME model results at E:\2015-tm22-dev-sprint-04
    - tm2pyenv-acceptance Python environment
    - EMME API access via inro.emme modules
"""

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
import numpy as np

# Add tm2py to path
sys.path.insert(0, r'c:\GitHub\tm2py')


class NetworkSummarizer:
    """Generates comprehensive network performance summaries from EMME results."""
    
    def __init__(self, model_run_dir: str, output_dir: Optional[str] = None):
        """
        Initialize the network summarizer.
        
        Args:
            model_run_dir: Path to the model run directory
            output_dir: Optional output directory (defaults to model_run_dir/network_summary)
        """
        self.model_run_dir = Path(model_run_dir)
        self.output_dir = Path(output_dir) if output_dir else self.model_run_dir / "network_summary"
        
        # Set up logging
        self._setup_logging()
        
        # Initialize database connection
        self.highway_bank = None
        
        # Time period mapping
        self.time_period_mapping = {
            'ea': '20to5',  # Early AM (overnight for freeflow)
            'am': '7to8',   # AM peak
            'md': '10to14', # Midday
            'pm': '17to18', # PM peak  
            'ev': '19to20'  # Evening
        }
        
        # Facility type mapping (data3 field)
        self.facility_type_mapping = {
            1: 'freeway',
            2: 'highway', 
            3: 'arterial',
            4: 'arterial',
            5: 'connector',
            6: 'arterial'
        }
        
        # County mapping (simplified - can be expanded)
        self.county_mapping = {
            1: 'San Francisco',
            2: 'San Mateo', 
            3: 'Santa Clara',
            4: 'Alameda',
            5: 'Contra Costa',
            6: 'Solano',
            7: 'Napa',
            8: 'Sonoma',
            9: 'Marin'
        }
        
    def _setup_logging(self) -> None:
        """Configure logging for the summarizer."""
        self.output_dir.mkdir(exist_ok=True)
        
        log_file = self.output_dir / "network_summary.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        
    def _connect_to_emme_database(self) -> bool:
        """Connect to EMME highway database."""
        try:
            from inro.emme.database.emmebank import Emmebank
            
            highway_db_path = self.model_run_dir / "emme_project" / "Database_highway" / "emmebank"
            self.highway_bank = Emmebank(str(highway_db_path))
            
            self.logger.info("Successfully connected to EMME highway database")
            return True
            
        except ImportError as e:
            self.logger.error(f"EMME modules not available: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Failed to connect to EMME database: {e}")
            return False
    
    def run(self) -> bool:
        """Execute the complete network summarization workflow."""
        self.logger.info("Starting TM2PY network performance summarization")
        self.logger.info(f"Model directory: {self.model_run_dir}")
        self.logger.info(f"Output directory: {self.output_dir}")
        
        if not self.model_run_dir.exists():
            self.logger.error(f"Model directory does not exist: {self.model_run_dir}")
            return False
        
        if not self._connect_to_emme_database():
            return False
        
        try:
            # Extract link-level data across all time periods
            link_data = self._extract_all_time_periods()
            
            if link_data.empty:
                self.logger.error("No link data extracted")
                return False
            
            # Generate summary reports
            self._summarize_network_performance(link_data)
            
            self.logger.info("Network summarization completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Summarization failed: {e}", exc_info=True)
            return False
    
    def _extract_all_time_periods(self) -> pd.DataFrame:
        """Extract link-level data for all time periods."""
        self.logger.info("Extracting link data for all time periods")
        
        all_data = []
        scenarios = list(self.highway_bank.scenarios())
        
        for scenario_id in scenarios:
            scenario = self.highway_bank.scenario(scenario_id)
            time_period = self._map_scenario_to_time_period(scenario)
            
            if time_period == 'unknown':
                self.logger.warning(f"Skipping scenario {scenario_id}: unknown time period")
                continue
                
            self.logger.info(f"Processing scenario {scenario_id}: {scenario.title} -> {time_period}")
            
            try:
                network = scenario.get_network()
                scenario_data = self._extract_scenario_links(network, time_period)
                all_data.extend(scenario_data)
                
            except Exception as e:
                self.logger.warning(f"Failed to process scenario {scenario_id}: {e}")
        
        if not all_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(all_data)
        self.logger.info(f"Extracted {len(df)} link records across {df['time_period'].nunique()} time periods")
        
        return df
    
    def _map_scenario_to_time_period(self, scenario) -> str:
        """Map scenario title to standard time period."""
        title = scenario.title.lower() if scenario.title else ""
        
        if 'am' in title or 'morning' in title:
            return 'am'
        elif 'pm' in title or 'afternoon' in title:
            return 'pm'
        elif 'md' in title or 'midday' in title:
            return 'md'
        elif 'ea' in title or 'early' in title:
            return 'ea'
        elif 'ev' in title or 'night' in title:
            return 'ev'
        else:
            return 'unknown'
    
    def _extract_scenario_links(self, network, time_period: str) -> List[Dict]:
        """Extract link data for a single scenario."""
        data = []
        
        for link in network.links():
            # Get key link attributes
            volume = getattr(link, 'auto_volume', 0) or getattr(link, 'volume', 0)
            auto_time = getattr(link, 'auto_time', 0)
            length = link.length
            num_lanes = getattr(link, 'num_lanes', 1)
            capacity = getattr(link, 'capacity', 0)
            
            # Get facility type from data3
            facility_type_code = getattr(link, 'data3', 0)
            facility_type = self.facility_type_mapping.get(facility_type_code, 'other')
            
            # Get county ID (assuming from data1 or similar)
            county_id = getattr(link, 'data1', 0)
            county_name = self.county_mapping.get(county_id, 'Outside Region')
            
            # Create link identifier
            link_id = f"{link.i_node.id}_{link.j_node.id}"
            
            data.append({
                'link_id': link_id,
                'i_node': int(link.i_node.id),
                'j_node': int(link.j_node.id),
                'time_period': time_period,
                'volume': volume,
                'auto_time': auto_time,
                'length': length,
                'num_lanes': num_lanes,
                'capacity': capacity,
                'facility_type_code': facility_type_code,
                'facility_type': facility_type,
                'county_id': county_id,
                'county_name': county_name
            })
        
        return data
    
    def _summarize_network_performance(self, df: pd.DataFrame) -> None:
        """Generate comprehensive network performance summaries."""
        self.logger.info("Generating network performance summaries")
        
        # Exclude non-designated facilities (facility_type_code == 0)
        df = df[df['facility_type_code'] != 0].copy()
        
        # Calculate basic metrics
        df = self._calculate_performance_metrics(df)
        
        # Create Excel writer for multiple sheets
        excel_file = self.output_dir / "network_performance_summary.xlsx"
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            
            # Summary by facility type
            self._summarize_by_facility_type(df, writer)
            
            # Summary by time period
            self._summarize_by_time_period(df, writer)
            
            # County-level summaries
            self._summarize_by_county(df, writer)
            
            # Lane mile inventory
            self._generate_lane_mile_inventory(df, writer)
            
            # Overall summary
            self._generate_overall_summary(df, writer)
        
        self.logger.info(f"Network summary saved: {excel_file}")
    
    def _calculate_performance_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate VMT, VHT, and delay metrics."""
        self.logger.info("Calculating performance metrics")
        
        # Calculate VMT and VHT
        df['vmt'] = df['volume'] * df['length']
        df['vht'] = df['volume'] * df['auto_time'] / 60  # Convert to hours
        
        # Calculate lane miles
        df['lane_miles'] = df['length'] * df['num_lanes']
        
        # Calculate delay (using overnight as freeflow)
        freeflow_data = df[df['time_period'] == 'ea'][['link_id', 'auto_time']].copy()
        freeflow_data.rename(columns={'auto_time': 'freeflow_time'}, inplace=True)
        
        # Merge freeflow times
        df = df.merge(freeflow_data, on='link_id', how='left')
        
        # Calculate delay (handle missing freeflow times)
        df['freeflow_time'] = df['freeflow_time'].fillna(df['auto_time'])
        df['delay_per_vehicle'] = np.maximum(0, df['auto_time'] - df['freeflow_time'])
        df['total_delay'] = (df['delay_per_vehicle'] * df['volume']) / 60  # Hours
        
        # Calculate speeds
        df['speed_mph'] = np.where(df['auto_time'] > 0, 
                                  (df['length'] / df['auto_time']) * 3600 / 5280, 
                                  0)
        
        return df
    
    def _summarize_by_facility_type(self, df: pd.DataFrame, writer) -> None:
        """Generate summaries by facility type."""
        metrics = ['vmt', 'vht', 'total_delay']
        
        for metric in metrics:
            summary = pd.pivot_table(
                df,
                values=metric,
                index=['time_period'],
                columns='facility_type',
                aggfunc='sum'
            ).reset_index()
            
            # Sort by time period
            time_order = ['ea', 'am', 'md', 'pm', 'ev']
            summary['time_period'] = pd.Categorical(summary['time_period'], categories=time_order, ordered=True)
            summary = summary.sort_values('time_period').reset_index(drop=True)
            
            # Save to Excel and CSV
            sheet_name = f"{metric.upper()} by Facility"
            summary.to_excel(writer, sheet_name=sheet_name, index=False)
            
            csv_file = self.output_dir / f"{metric}_by_facility.csv"
            summary.to_csv(csv_file, index=False)
            
            self.logger.info(f"Generated {metric} by facility type summary")
    
    def _summarize_by_time_period(self, df: pd.DataFrame, writer) -> None:
        """Generate summaries by time period."""
        summary = df.groupby('time_period').agg({
            'vmt': 'sum',
            'vht': 'sum', 
            'total_delay': 'sum',
            'volume': 'sum',
            'length': lambda x: (x * df.loc[x.index, 'volume']).sum() / df.loc[x.index, 'volume'].sum()  # Weighted avg
        }).reset_index()
        
        # Add average speed
        summary['avg_speed_mph'] = (summary['vmt'] / summary['vht']).fillna(0)
        
        # Sort by time period
        time_order = ['ea', 'am', 'md', 'pm', 'ev']
        summary['time_period'] = pd.Categorical(summary['time_period'], categories=time_order, ordered=True)
        summary = summary.sort_values('time_period').reset_index(drop=True)
        
        summary.to_excel(writer, sheet_name="Summary by Time Period", index=False)
        
        csv_file = self.output_dir / "summary_by_time_period.csv"
        summary.to_csv(csv_file, index=False)
        
        self.logger.info("Generated time period summary")
    
    def _summarize_by_county(self, df: pd.DataFrame, writer) -> None:
        """Generate county-level summaries."""
        county_summary = df.groupby('county_name').agg({
            'vmt': 'sum',
            'vht': 'sum',
            'total_delay': 'sum',
            'volume': 'sum'
        }).reset_index()
        
        county_summary['avg_speed_mph'] = (county_summary['vmt'] / county_summary['vht']).fillna(0)
        
        county_summary.to_excel(writer, sheet_name="County Summary", index=False)
        
        csv_file = self.output_dir / "county_summary.csv"
        county_summary.to_csv(csv_file, index=False)
        
        self.logger.info("Generated county summary")
    
    def _generate_lane_mile_inventory(self, df: pd.DataFrame, writer) -> None:
        """Generate lane mile inventory by county and facility type."""
        # Use one time period to avoid double counting
        inventory_df = df[df['time_period'] == 'am'].copy()
        
        lane_miles = pd.pivot_table(
            inventory_df,
            values='lane_miles',
            index='county_name',
            columns='facility_type',
            aggfunc='sum'
        ).reset_index()
        
        # Add VMT for context
        vmt_summary = df.groupby('county_name')['vmt'].sum().reset_index()
        vmt_summary.rename(columns={'vmt': 'total_daily_vmt'}, inplace=True)
        
        lane_miles = lane_miles.merge(vmt_summary, on='county_name', how='left')
        
        lane_miles.to_excel(writer, sheet_name="Lane Mile Inventory", index=False)
        
        csv_file = self.output_dir / "lane_mile_inventory.csv"
        lane_miles.to_csv(csv_file, index=False)
        
        self.logger.info("Generated lane mile inventory")
    
    def _generate_overall_summary(self, df: pd.DataFrame, writer) -> None:
        """Generate overall system summary."""
        total_summary = {
            'Total Daily VMT': df['vmt'].sum(),
            'Total Daily VHT': df['vht'].sum(),
            'Total Daily Delay (hours)': df['total_delay'].sum(),
            'Average System Speed (mph)': df['vmt'].sum() / df['vht'].sum() if df['vht'].sum() > 0 else 0,
            'Total Lane Miles': df[df['time_period'] == 'am']['lane_miles'].sum(),
            'Peak Hour VMT (AM)': df[df['time_period'] == 'am']['vmt'].sum(),
            'Peak Hour VMT (PM)': df[df['time_period'] == 'pm']['vmt'].sum(),
        }
        
        summary_df = pd.DataFrame(list(total_summary.items()), columns=['Metric', 'Value'])
        summary_df.to_excel(writer, sheet_name="Overall Summary", index=False)
        
        csv_file = self.output_dir / "overall_summary.csv"
        summary_df.to_csv(csv_file, index=False)
        
        self.logger.info("Generated overall summary")


def main() -> int:
    """Main entry point for the network summarizer."""
    model_run_dir = r"E:\2015-tm22-dev-sprint-04"
    
    summarizer = NetworkSummarizer(model_run_dir)
    success = summarizer.run()
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())