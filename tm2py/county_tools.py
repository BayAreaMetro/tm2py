"""Utilities for county-specific highway testing and data filtering.

This module provides utilities for filtering TM2 model data to specific counties
and setting up county-specific test environments.

Key features:
1. Auto-detect zone ranges (TAZ/MAZ) for counties from crosswalk files
2. Filter trip tables, MAZ data, and network files to county zones
3. Set up county-specific test directories with filtered data

Example usage:
    from tm2py.tools.county_utils import get_county_zones, CountyDataFilter
    
    # Auto-detect zones for a county
    zones = get_county_zones("San Mateo")
    
    # Create filter and filter trip data
    filter_helper = CountyDataFilter(
        taz_range=zones['taz_range'],
        maz_range=zones['maz_range'],
        county_name="San Mateo"
    )
    filter_helper.filter_trip_table(input_omx, output_omx)
"""

import os
import shutil
import sys
from pathlib import Path
from typing import List, Tuple, Optional, Dict

import numpy as np
import openmatrix as omx
import pandas as pd

from tm2py.controller import RunController


# Path to TAZ/MAZ/County crosswalk file
# NOTE: Using newer crosswalk - may need to find old crosswalk file from E:\2015_TM2_20250619 dataset
CROSSWALK_FILE = Path(r"C:\GitHub\tm2py-utils\tm2py_utils\inputs\maz_taz\mazs_tazs_county_tract_PUMA_2.5.csv")

# Default zone ranges (override these based on your zone system)
# Or use get_county_zones() to auto-detect from crosswalk
DEFAULT_TAZ_RANGE = (200, 400)  # Update with your county's TAZ range
DEFAULT_MAZ_RANGE = (2000, 4000)  # Update with your county's MAZ range


def get_county_zones(county_name: str, crosswalk_file: Optional[Path] = None) -> Dict[str, Tuple[int, int]]:
    """Auto-detect TAZ and MAZ ranges for a county from crosswalk file.
    
    Args:
        county_name: Name of the county (e.g., "San Mateo", "Alameda")
        crosswalk_file: Optional path to crosswalk CSV. Uses CROSSWALK_FILE if not provided.
        
    Returns:
        Dictionary with 'taz_range' and 'maz_range' tuples
        
    Example:
        >>> zones = get_county_zones("San Mateo")
        >>> print(zones)
        {'taz_range': (200, 400), 'maz_range': (2000, 4000)}
    """
    crosswalk_path = crosswalk_file or CROSSWALK_FILE
    
    if not crosswalk_path.exists():
        raise FileNotFoundError(
            f"Crosswalk file not found: {crosswalk_path}\n"
            f"Please update CROSSWALK_FILE path or provide crosswalk_file parameter"
        )
    
    df = pd.read_csv(crosswalk_path)
    
    # Filter to specified county
    county_df = df[df['county_name'].str.contains(county_name, case=False, na=False)]
    
    if len(county_df) == 0:
        available_counties = df['county_name'].unique()
        raise ValueError(
            f"No zones found for county: {county_name}\n"
            f"Available counties: {', '.join(sorted(available_counties))}"
        )
    
    # Get TAZ range (using TAZ_SEQ for sequential IDs)
    taz_min = int(county_df['TAZ_SEQ'].min())
    taz_max = int(county_df['TAZ_SEQ'].max())
    
    # Get MAZ range (using MAZ_SEQ for sequential IDs)
    maz_min = int(county_df['MAZ_SEQ'].min())
    maz_max = int(county_df['MAZ_SEQ'].max())
    
    print(f"\nDetected zones for {county_name} County:")
    print(f"  TAZ range: {taz_min} - {taz_max} ({taz_max - taz_min + 1} zones)")
    print(f"  MAZ range: {maz_min} - {maz_max} ({maz_max - maz_min + 1} zones)")
    
    return {
        'taz_range': (taz_min, taz_max),
        'maz_range': (maz_min, maz_max)
    }


class CountyDataFilter:
    """Helper class to filter model data to a specific county."""

    def __init__(self, taz_range: tuple, maz_range: tuple, county_name: Optional[str] = None):
        """Initialize the data filter.

        Args:
            taz_range: Tuple of (min_taz, max_taz) for the county
            maz_range: Tuple of (min_maz, max_maz) for the county
            county_name: Optional name of the county (for logging/display)
        """
        self.taz_min, self.taz_max = taz_range
        self.maz_min, self.maz_max = maz_range
        self.county_name = county_name or "County"

    def filter_trip_table(
        self, input_file: Path, output_file: Path, origin_col: str = "origin", 
        dest_col: str = "destination"
    ):
        """Filter trip table to only include trips within the county.

        Args:
            input_file: Path to input trip table (CSV or OMX)
            output_file: Path to output filtered trip table
            origin_col: Name of origin zone column
            dest_col: Name of destination zone column
        """
        if input_file.suffix == ".csv":
            df = pd.read_csv(input_file)
            # Filter to trips that start AND end in the county
            df_filtered = df[
                (df[origin_col] >= self.taz_min) & (df[origin_col] <= self.taz_max) &
                (df[dest_col] >= self.taz_min) & (df[dest_col] <= self.taz_max)
            ]
            df_filtered.to_csv(output_file, index=False)
            print(f"Filtered {len(df)} trips to {len(df_filtered)} {self.county_name} trips")
        elif input_file.suffix == ".omx":
            # Filter OMX matrix to county zones
            with omx.open_file(str(input_file), 'r') as omx_in:
                # Get zone mapping
                if len(omx_in.list_mappings()) > 0:
                    mapping_name = omx_in.list_mappings()[0]
                    zone_mapping = omx_in.mapping(mapping_name).keys()
                else:
                    # No mapping, assume sequential zones starting at 1
                    zone_mapping = list(range(1, omx_in.shape()[0] + 1))
                
                # Find indices for county zones
                county_indices = [
                    i for i, z in enumerate(zone_mapping)
                    if self.taz_min <= z <= self.taz_max
                ]
                county_zones = [
                    z for z in zone_mapping
                    if self.taz_min <= z <= self.taz_max
                ]
                
                if len(county_indices) == 0:
                    raise ValueError(
                        f"No zones found in range {self.taz_min}-{self.taz_max} in {input_file.name}"
                    )
                
                # Create output OMX with filtered data
                with omx.open_file(str(output_file), 'w') as omx_out:
                    # Create zone mapping for county only
                    omx_out.create_mapping('taz', county_zones)
                    
                    # Copy and filter each matrix
                    total_trips_in = 0
                    total_trips_out = 0
                    for matrix_name in omx_in.list_matrices():
                        matrix_data = omx_in[matrix_name][:]
                        total_trips_in += matrix_data.sum()
                        
                        # Extract submatrix for county zones only
                        filtered_data = matrix_data[np.ix_(county_indices, county_indices)]
                        total_trips_out += filtered_data.sum()
                        
                        omx_out[matrix_name] = filtered_data
                    
                    print(f"  {input_file.name}: {len(county_zones)} zones, "
                          f"{total_trips_in:,.0f} → {total_trips_out:,.0f} trips")

    def filter_maz_data(self, input_file: Path, output_file: Path):
        """Filter MAZ land use data to specified county only.

        Args:
            input_file: Path to input MAZ data CSV
            output_file: Path to output filtered MAZ data CSV
        """
        df = pd.read_csv(input_file)
        df_filtered = df[
            (df['MAZ'] >= self.maz_min) & (df['MAZ'] <= self.maz_max)
        ]
        df_filtered.to_csv(output_file, index=False)
        print(f"Filtered {len(df)} MAZs to {len(df_filtered)} {self.county_name} MAZs")

    def filter_network_links(self, input_file: Path, output_file: Path):
        """Filter highway network links to those within/connected to the county.

        Args:
            input_file: Path to input network file
            output_file: Path to output filtered network file
        """
        # This is simplified - you may need more sophisticated filtering
        # to keep through-routes and connections to adjacent counties
        df = pd.read_csv(input_file)
        # Keep links where at least one node is in county range
        # This is a placeholder - adjust based on your network structure
        df.to_csv(output_file, index=False)


def filter_ctramp_highway_trips(
    ctramp_output_dir: Path,
    test_demand_dir: Path,
    filter_helper: CountyDataFilter,
    time_periods: Optional[List[str]] = None
) -> None:
    """Filter highway trip OMX files from CTRAMP output to county subset.
    
    This function finds all highway trip OMX files in the ctramp_output directory
    and filters them to only include O-D pairs where both origin and destination
    are within the county TAZ range.
    
    Args:
        ctramp_output_dir: Path to CTRAMP output directory (e.g., E:\\2023-tm22-dev-version-05\\ctramp_output)
        test_demand_dir: Path to test demand directory where filtered files will be written
        filter_helper: CountyDataFilter instance with TAZ range
        time_periods: Optional list of time periods to filter (e.g., ['EA', 'AM', 'MD', 'PM', 'EV'])
                     If None, filters all OMX files found
    
    Example:
        >>> filter_helper = CountyDataFilter((200, 400), (2000, 4000), "San Mateo")
        >>> filter_ctramp_highway_trips(
        ...     Path("E:/2023-tm22-dev-version-05/ctramp_output"),
        ...     Path("test_san_mateo/inputs/demand"),
        ...     filter_helper,
        ...     time_periods=['AM', 'PM']
        ... )
    """
    test_demand_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all OMX files in ctramp_output
    omx_files = list(ctramp_output_dir.glob("*.omx"))
    
    if not omx_files:
        raise FileNotFoundError(f"No OMX files found in {ctramp_output_dir}")
    
    print(f"\nFiltering highway trip files from CTRAMP output:")
    print(f"  Source: {ctramp_output_dir}")
    print(f"  TAZ range: {filter_helper.taz_min}-{filter_helper.taz_max}")
    
    filtered_count = 0
    for omx_file in omx_files:
        # Filter by time period if specified
        if time_periods:
            if not any(tp in omx_file.stem for tp in time_periods):
                continue
        
        output_file = test_demand_dir / omx_file.name
        
        try:
            filter_helper.filter_trip_table(omx_file, output_file)
            filtered_count += 1
        except Exception as e:
            print(f"  Warning: Could not filter {omx_file.name}: {e}")
    
    print(f"\n✓ Filtered {filtered_count} trip files to {test_demand_dir}")


def setup_county_test_data(
    source_dir: Path, 
    test_dir: Path,
    filter_helper: CountyDataFilter
) -> Path:
    """Set up filtered test data for a specific county.

    Args:
        source_dir: Source directory with full model data
        test_dir: Test directory where filtered data will be created
        filter_helper: CountyDataFilter instance

    Returns:
        Path to the test directory with filtered data
    """
    test_dir.mkdir(parents=True, exist_ok=True)

    # Create directory structure
    (test_dir / "inputs" / "hwy").mkdir(parents=True, exist_ok=True)
    (test_dir / "inputs" / "landuse").mkdir(parents=True, exist_ok=True)
    (test_dir / "inputs" / "demand").mkdir(parents=True, exist_ok=True)
    (test_dir / "emme_project").mkdir(parents=True, exist_ok=True)

    # Filter MAZ data
    if (source_dir / "inputs" / "landuse" / "maz_data.csv").exists():
        filter_helper.filter_maz_data(
            source_dir / "inputs" / "landuse" / "maz_data.csv",
            test_dir / "inputs" / "landuse" / "maz_data.csv"
        )

    # Copy highway network files (these may need filtering depending on your needs)
    hwy_files = ["tolls.csv", "highway.net", "turn_restrictions.csv"]
    for hwy_file in hwy_files:
        src_file = source_dir / "inputs" / "hwy" / hwy_file
        if src_file.exists():
            shutil.copy(src_file, test_dir / "inputs" / "hwy" / hwy_file)

    # Note: You'll need to add your fixed trip tables here
    # filter_helper.filter_trip_table(
    #     source_dir / "inputs" / "demand" / "trips.omx",
    #     test_dir / "inputs" / "demand" / "trips_county.omx"
    # )

    return test_dir
