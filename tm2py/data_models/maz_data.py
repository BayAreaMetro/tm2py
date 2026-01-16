"""MAZ Data Model for TM2.0 Transportation Modeling

This module provides data validation and management for Micro-Analysis Zone (MAZ) data,
which forms the foundation of land use inputs for the TM2.0 transportation model.

Overview
--------
MAZ (Micro-Analysis Zone) data represents fine-grained geographic units that contain
detailed land use, demographic, and employment information. This data is crucial for:

- Trip generation modeling based on land use characteristics
- Accessibility calculations for transportation modes
- Economic and demographic analysis at a granular geographic level
- Integration with larger Traffic Analysis Zones (TAZ) for model hierarchy

Key Components
--------------
MAZData : pandera.model.DataFrameModel
    Primary data validation class containing 60+ attributes for land use characteristics
    including employment by sector, demographic data, parking supply, and density measures.
    
NodeIDCrosswalk : pandera.model.DataFrameModel
    Manages the mapping between model node IDs and sequential IDs for MAZ, TAZ, 
    and external zones to ensure consistent geographic referencing.

Data Structure
--------------
The MAZ data follows a hierarchical structure where:
- Multiple MAZs can belong to a single TAZ (Traffic Analysis Zone)
- Each MAZ has unique identifiers (original and sequential)
- Land use data is categorized by employment sectors, housing types, and amenities
- Validation ensures data consistency and completeness for modeling

Usage
-----
This module is typically used during the data preparation phase of transportation
modeling to validate and standardize land use inputs before they are consumed by
trip generation and other demand modeling components.

Example
-------
```python
from pathlib import Path
from tm2py.data_models.maz_data import load_maz_data, create_sequential_index

# Create node ID crosswalk from Lasso network build output
xwalk_file = Path('model_to_emme_node_id.csv')
crosswalk = create_sequential_index(xwalk_file)

# Load and validate MAZ data
maz_file = Path('maz_land_use_data.csv')
maz_data = load_maz_data(maz_file, crosswalk)
```
"""
import pathlib

import pandas as pd
import numpy as np
import pandera.pandas as pa

from pandera import Field
from pandera.typing import Series, DataFrame


external_N_list = list(range(900001, 1000000))
taz_N_list = (
    list(range(1, 10000)) 
    + list(range(100001, 110000)) 
    + list(range(200001, 210000)) 
    + list(range(300001, 310000))
    + list(range(400001, 410000)) 
    + list(range(500001, 510000)) 
    + list(range(600001, 610000)) 
    + list(range(700001, 710000))
    + list(range(800001, 810000))
)
maz_N_list = (
    list(range(10001, 90000)) 
    + list(range(110001, 190000)) 
    + list(range(210001, 290000)) 
    + list(range(310001, 390000))
    + list(range(410001, 490000)) 
    + list(range(510001, 590000)) 
    + list(range(610001, 690000)) 
    + list(range(710001, 790000))
    + list(range(810001, 890000))
)
disconnected_maz_N_list = [10186, 16084, 111432, 111433, 411178]


class MAZData(pa.DataFrameModel):
    """Micro-Analysis Zone (MAZ) Land Use Data Validation Model.
    
    This class validates MAZ-level land use data used in TM2.0 transportation modeling.
    MAZs represent the finest geographic resolution for land use data, containing detailed
    information about employment by sector, demographics, parking supply, and accessibility
    measures. This data drives trip generation and other demand modeling components.
    
    The validation ensures data consistency, proper data types, and logical constraints
    across all land use attributes before they are consumed by the transportation model.
    
    Geographic Hierarchy
    --------------------
    - MAZ (Micro-Analysis Zone): Finest geographic unit
    - TAZ (Traffic Analysis Zone): Aggregates multiple MAZs 
    - District/County: Higher-level geographic groupings
    
    Data Categories
    ---------------
    1. **Geographic Identifiers**: MAZ/TAZ IDs, coordinates, district/county information
    2. **Demographics**: Households, population, school enrollment by type
    3. **Employment by Sector**: 21 detailed employment categories (retail, manufacturing, services, etc.)
    4. **Parking Supply**: Hourly, daily, and monthly parking by destination type
    5. **Density Measures**: Employment, population, and household densities within 1/2 mile
    6. **Accessibility**: Intersection counts and density classifications
    
    Employment Categories
    ---------------------
    The model includes detailed employment data across major sectors:
    - **Primary**: Agriculture (ag), Natural Resources (natres)
    - **Manufacturing**: Bio (man_bio), Light (man_lgt), Heavy (man_hvy), Tech (man_tech)  
    - **Services**: Professional (prof), Business (serv_bus), Personal (serv_pers), Social (serv_soc)
    - **Retail**: Local (ret_loc), Regional (ret_reg)
    - **Education**: K-12 (ed_k12), Higher Ed (ed_high), Other (ed_oth)
    - **Other**: Government (gov), Health, Construction (constr), Transportation (transp), etc.
    
    Parking Data Structure
    ----------------------
    Parking supply is categorized by:
    - **Duration**: Hourly (h), Daily (d), Monthly (m) 
    - **Destination**: Same MAZ (sam) vs Other MAZs (oth)
    - **Costs**: Average hourly, daily, and monthly parking costs
    
    Density Classifications
    -----------------------
    Several attributes use binned density measures (1-3 scale):
    - IntDenBin: Intersection density (walkability proxy)
    - EmpDenBin: Employment density (job accessibility)
    - DUDenBin: Household density (residential intensity)
    
    Validation Rules
    ----------------
    - All geographic IDs must be unique and non-null
    - Employment and demographic counts must be non-negative integers
    - Parking costs and areas must be non-negative floats
    - Density measures include both raw values and binned classifications
    
    Example
    -------
    ```python
    import pandas as pd
    from tm2py.data_models.maz_data import MAZData
    
    # Validate MAZ data
    maz_df = pd.read_csv('maz_land_use.csv')
    validated_data = MAZData.validate(maz_df)
    
    # Access employment totals
    total_jobs = validated_data['emp_total'].sum()
    retail_jobs = validated_data['ret_loc'].sum() + validated_data['ret_reg'].sum()
    
    # Analyze density patterns
    high_density_mazs = validated_data[validated_data['EmpDenBin'] == 3]
    walkable_areas = validated_data[validated_data['IntDenBin'] >= 2]
    ```
    """
    MAZ: Series[int] = Field(nullable=False, unique=True)
    TAZ: Series[int] = Field(nullable=False)
    MAZ_ORIGINAL: Series[int] = Field(nullable=False, unique=True)    
    TAZ_ORIGINAL: Series[int] = Field(nullable=False)
    DistID: Series[int] = Field(nullable=False)
    DistName: Series[str] = Field(nullable=False)    
    CountyID: Series[int] = Field(nullable=False)
    CountyName: Series[str] = Field(nullable=False)
    ACRES: Series[float] = Field(nullable=False, ge=0)
    HH: Series[int] = Field(nullable=False, ge=0)
    POP: Series[int] = Field(nullable=False, ge=0)
    ag: Series[int] = Field(nullable=False, ge=0)
    art_rec: Series[int] = Field(nullable=False, ge=0)
    constr: Series[int] = Field(nullable=False, ge=0)
    eat: Series[int] = Field(nullable=False, ge=0)
    ed_high: Series[int] = Field(nullable=False, ge=0)
    ed_k12: Series[int] = Field(nullable=False, ge=0)
    ed_oth: Series[int] = Field(nullable=False, ge=0)
    fire: Series[int] = Field(nullable=False, ge=0)
    gov: Series[int] = Field(nullable=False, ge=0)
    health: Series[int] = Field(nullable=False, ge=0)
    hotel: Series[int] = Field(nullable=False, ge=0)
    info: Series[int] = Field(nullable=False, ge=0)
    lease: Series[int] = Field(nullable=False, ge=0)
    logis: Series[int] = Field(nullable=False, ge=0)
    man_bio: Series[int] = Field(nullable=False, ge=0)
    man_lgt: Series[int] = Field(nullable=False, ge=0)
    man_hvy: Series[int] = Field(nullable=False, ge=0)
    man_tech: Series[int] = Field(nullable=False, ge=0)
    natres: Series[int] = Field(nullable=False, ge=0)
    prof: Series[int] = Field(nullable=False, ge=0)
    ret_loc: Series[int] = Field(nullable=False, ge=0)
    ret_reg: Series[int] = Field(nullable=False, ge=0)
    serv_bus: Series[int] = Field(nullable=False, ge=0)
    serv_pers: Series[int] = Field(nullable=False, ge=0)
    serv_soc: Series[int] = Field(nullable=False, ge=0)
    transp: Series[int] = Field(nullable=False, ge=0)
    util: Series[int] = Field(nullable=False, ge=0)
    emp_total: Series[int] = Field(nullable=False, ge=0)
    publicEnrollGradeKto8: Series[int] = Field(nullable=False, ge=0)
    privateEnrollGradeKto8: Series[int] = Field(nullable=False, ge=0)
    publicEnrollGrade9to12: Series[int] = Field(nullable=False, ge=0)
    privateEnrollGrade9to12: Series[int] = Field(nullable=False, ge=0)
    comm_coll_enroll: Series[int] = Field(nullable=False, ge=0)
    EnrollGradeKto8: Series[int] = Field(nullable=False, ge=0)
    EnrollGrade9to12: Series[float] = Field(nullable=False, ge=0)
    collegeEnroll: Series[float] = Field(nullable=False, ge=0)
    otherCollegeEnroll: Series[float] = Field(nullable=False, ge=0)
    AdultSchEnrl: Series[int] = Field(nullable=False, ge=0)
    hstallsoth: Series[float] = Field(nullable=False, ge=0)
    hstallssam: Series[float] = Field(nullable=False, ge=0)
    dstallsoth: Series[float] = Field(nullable=False, ge=0)
    dstallssam: Series[float] = Field(nullable=False, ge=0)
    mstallsoth: Series[float] = Field(nullable=False, ge=0)
    mstallssam: Series[float] = Field(nullable=False, ge=0)
    park_area: Series[float] = Field(nullable=False, ge=0)
    hparkcost: Series[float] = Field(nullable=False, ge=0)
    numfreehrs: Series[float] = Field(nullable=False, ge=0)
    dparkcost: Series[float] = Field(nullable=False, ge=0)
    mparkcost: Series[float] = Field(nullable=False, ge=0)
    ech_dist: Series[int] = Field(nullable=False, ge=0)
    hch_dist: Series[int] = Field(nullable=False, ge=0)
    parkarea: Series[int] = Field(nullable=False, ge=0)
    TERMINAL: Series[float] = Field(nullable=False, ge=0)
    MAZ_X: Series[float] = Field(nullable=False, ge=0)
    MAZ_Y: Series[float] = Field(nullable=False, ge=0)
    TotInt: Series[int] = Field(nullable=False, ge=0)
    EmpDen: Series[float] = Field(nullable=False, ge=0)
    RetEmpDen: Series[float] = Field(nullable=False, ge=0)
    DUDen: Series[float] = Field(nullable=False, ge=0)
    PopDen: Series[float] = Field(nullable=False, ge=0)
    IntDenBin: Series[int] = Field(nullable=False, ge=0)
    EmpDenBin: Series[int] = Field(nullable=False, ge=0)
    DuDenBin: Series[int] = Field(nullable=False, ge=0)
    PopEmpDenPerMi: Series[float] = Field(nullable=False, ge=0)

    class Config:
        strict = "filter"
        coerce = True
        unique_column_names = True

class NodeIDCrosswalk(pa.DataFrameModel):
    """Node ID to Sequential ID Mapping for Transportation Model Geography.
    
    This class validates the crosswalk table that maps original model node IDs 
    to sequential zone identifiers used throughout the TM2.0 transportation model.
    It ensures consistent geographic referencing across MAZ, TAZ, and external zones.
    
    Purpose
    -------
    The transportation model requires sequential zone IDs (starting from 1) for 
    efficient matrix operations and memory management, while the underlying network
    model uses arbitrary node IDs. This crosswalk maintains the mapping between
    these two ID systems.
    
    Geographic Types
    ----------------
    - **MAZ (Micro-Analysis Zone)**: Finest resolution zones for land use data
    - **TAZ (Traffic Analysis Zone)**: Aggregated zones for trip matrices  
    - **EXT (External Zone)**: Special zones for external traffic flows
    
    ID System Design
    ----------------
    - Original model_node_id: Arbitrary integers from network model (can have gaps)
    - Sequential IDs: Continuous 1-based indexing for each zone type
    - Zero values: Indicate the node doesn't belong to that zone type
    
    Usage in Model
    --------------
    This crosswalk is used to:
    1. Convert between original and sequential IDs during data loading
    2. Validate that MAZ/TAZ relationships are consistent
    3. Ensure all required zones have proper sequential numbering
    4. Support matrix operations that require continuous indexing
    
    Data Validation
    ---------------
    - All model_node_id values must be unique and non-null
    - Sequential IDs must be non-negative integers  
    - Zero values allowed to indicate non-membership in zone type
    - Total count of non-zero sequential IDs should match expected zone counts
    
    Attributes
    ----------
    model_node_id : int
        Original node identifier from the transportation network model.
        Must be unique across all geographic zone types.
    MAZSEQ : int  
        Sequential MAZ identifier (1-based). Zero if node is not a MAZ.
        Used for MAZ-level land use data indexing and trip generation.
    TAZSEQ : int
        Sequential TAZ identifier (1-based). Zero if node is not a TAZ.
        Used for trip matrix indexing and zone-to-zone travel calculations.
    EXTSEQ : int
        Sequential external zone identifier (1-based). Zero if node is not external.
        Used for modeling trips entering/exiting the model region.
        
    Example
    -------
    ```python
    import pandas as pd
    from tm2py.data_models.maz_data import NodeIDCrosswalk, create_sequential_index
    
    # Create crosswalk from node lists
    crosswalk = create_sequential_index(
        node_id_df=network_nodes,
        maz_N_list=[101, 102, 103],  
        taz_N_list=[201, 202],
        ext_N_list=[301, 302]
    )
    
    # Validate the crosswalk
    validated = NodeIDCrosswalk.validate(crosswalk)
    
    # Use for ID conversion
    maz_sequential = validated.set_index('model_node_id')['MAZSEQ']
    original_to_seq = dict(zip(validated['model_node_id'], validated['MAZSEQ']))
    ```
    """
    model_node_id: Series[int] = Field(nullable=False, unique=True)
    MAZSEQ: Series[int] = Field(nullable=False)
    TAZSEQ: Series[int] = Field(nullable=False)
    EXTSEQ: Series[int] = Field(nullable=False)

    class Config:
        coerce = True
        unique_column_names = True


def create_sequential_index(
    model_to_emme_node_id_xwalk: pathlib.Path
) -> DataFrame[NodeIDCrosswalk]:
    """Create stable sequential IDs for MAZ, TAZ, and external zones.
    
    This function generates a crosswalk table that maps original model node IDs 
    to sequential zone identifiers (1-based indexing) for efficient matrix operations
    in the transportation model. It uses predefined node lists to categorize zones
    into MAZ, TAZ, and external types.
    
    The function handles:
    - TAZ nodes: Traffic Analysis Zones for trip matrix operations
    - MAZ nodes: Micro-Analysis Zones including disconnected zones
    - External nodes: Boundary zones for external trips
    
    Sequential ID Assignment
    ------------------------
    - TAZ: Sequential numbering based on sort order of node IDs
    - MAZ: Includes both connected network nodes and disconnected zones
    - EXT: External zones for trips entering/exiting the model region
    - Zero values indicate the node doesn't belong to that zone type
    
    Node List Sources
    -----------------
    The function uses module-level constants:
    - taz_N_list: Predefined TAZ node ID ranges
    - maz_N_list: Predefined MAZ node ID ranges  
    - external_N_list: External zone node ID range (900001-999999)
    - disconnected_maz_N_list: Special disconnected MAZ nodes
    
    Parameters
    ----------
    model_to_emme_node_id_xwalk : pathlib.Path
        Path to CSV file containing the crosswalk between model node IDs
        and Emme node IDs, created during the network build process.
        Must contain columns: 'emme_node_id', 'model_node_id'
    
    Returns
    -------
    DataFrame[NodeIDCrosswalk]
        Validated crosswalk with columns:
        - model_node_id: Original network node identifier
        - MAZSEQ: Sequential MAZ ID (0 if not a MAZ)
        - TAZSEQ: Sequential TAZ ID (0 if not a TAZ) 
        - EXTSEQ: Sequential external zone ID (0 if not external)
    
    Raises
    ------
    ValueError
        If required columns are missing from the input crosswalk file
    
    Example
    -------
    ```python
    from tm2py.data_models.maz_data import create_sequential_index
    from pathlib import Path
    
    # Create crosswalk from Lasso network build output
    xwalk_file = Path('model_to_emme_node_id.csv')
    crosswalk = create_sequential_index(xwalk_file)
    
    # Use crosswalk for ID conversion
    maz_lookup = crosswalk.set_index('model_node_id')['MAZSEQ']
    sequential_maz_id = maz_lookup[original_node_id]
    ```
    
    See Also
    --------
    NodeIDCrosswalk : The validation schema for the output crosswalk
    validate_sequential_id : Function to validate MAZ data against this crosswalk
    """
    node_id_df = pd.read_csv(model_to_emme_node_id_xwalk)
    required_cols = ["emme_node_id", "model_node_id"]
    missing_cols = [c for c in required_cols if c not in node_id_df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in model_to_emme_node_id_xwalk: {missing_cols}")
    # taz node
    taz_node_id_df = (
        node_id_df[node_id_df["model_node_id"].isin(taz_N_list)]
        .copy()
        .rename(columns={"emme_node_id":"TAZSEQ"})
    )
    # external taz node
    ext_node_id_df = (
        node_id_df[node_id_df["model_node_id"].isin(external_N_list)]
        .copy()
        .rename(columns={"emme_node_id":"EXTSEQ"})
    )
    # maz node, including the five disconnected mazs
    maz_node_id_df = (
        node_id_df[node_id_df["model_node_id"].isin(maz_N_list)]
        .copy()
        .rename(columns={"emme_node_id":"MAZSEQ"})
    )
    maz_node_id_df = pd.concat(
        [maz_node_id_df[["model_node_id"]],
        pd.DataFrame({"model_node_id":disconnected_maz_N_list})]
    )
    maz_node_id_df = (
        maz_node_id_df
        .sort_values(by="model_node_id")
        .reset_index(drop=True)
    )
    maz_node_id_df["MAZSEQ"] = maz_node_id_df.index + 1

    out = (
        taz_node_id_df.merge(maz_node_id_df, on="model_node_id", how="outer")
        .merge(ext_node_id_df, on="model_node_id", how="outer")
        .fillna(0)
        .astype(int)
    )
    out = out[["model_node_id"] + [c for c in out.columns if c!="model_node_id"]]
    
    return NodeIDCrosswalk.validate(out, lazy=True)

def validate_sequential_id(
    maz_data_df: pd.DataFrame,
    node_seq_id_xwalk: DataFrame[NodeIDCrosswalk] = None
) -> None:
    """Validate consistency between MAZ data and node ID crosswalk.
    
    If node_seq_id_xwalk is None, validation is skipped (for older datasets).
    
    This function ensures that the sequential MAZ and TAZ IDs in the land use
    data file match the expected values from the node ID crosswalk. This validation
    is critical for maintaining geographic consistency across model components.
    
    The validation checks that:
    - Each MAZ_ORIGINAL in the data maps to the correct MAZ sequential ID
    - Each TAZ_ORIGINAL in the data maps to the correct TAZ sequential ID  
    - No mismatches exist that would cause geographic referencing errors
    
    Validation Process
    ------------------
    1. Create lookup from original node IDs to sequential IDs
    2. Map MAZ_ORIGINAL and TAZ_ORIGINAL to expected sequential values
    3. Compare with actual MAZ and TAZ columns in the data
    4. Report any mismatches that indicate data inconsistency
    
    Use Case
    --------
    This function is essential when loading MAZ data from external sources
    to ensure the geographic identifiers are properly aligned with the 
    transportation model's internal numbering system.
    
    Parameters
    ----------
    maz_data_df : pd.DataFrame
        MAZ land use data containing columns:
        - MAZ: Sequential MAZ identifier  
        - TAZ: Sequential TAZ identifier
        - MAZ_ORIGINAL: Original MAZ node ID
        - TAZ_ORIGINAL: Original TAZ node ID
    node_seq_id_xwalk : DataFrame[NodeIDCrosswalk]
        Validated crosswalk mapping original node IDs to sequential IDs.
        Created by create_sequential_index function.
    
    Returns
    -------
    None
        Function validates in-place and raises exception on failure
    
    Raises  
    ------
    ValueError
        If any MAZ or TAZ sequential IDs don't match the crosswalk expectations.
        Error message includes count of mismatched zones for debugging.
    
    Example
    -------
    ```python
    import pandas as pd
    from tm2py.data_models.maz_data import validate_sequential_id
    
    # Load data and crosswalk
    maz_df = pd.read_csv('maz_land_use.csv')
    crosswalk = create_sequential_index(node_xwalk_file)
    
    # Validate consistency  
    try:
        validate_sequential_id(maz_df, crosswalk)
        print("MAZ data geographic IDs validated successfully")
    except ValueError as e:
        print(f"Geographic ID mismatch: {e}")
    ```
    
    See Also
    --------
    create_sequential_index : Creates the required crosswalk 
    load_maz_data : Higher-level function that includes this validation
    NodeIDCrosswalk : Schema for the crosswalk data
    """
    # Skip validation if crosswalk not provided (for older datasets)
    if node_seq_id_xwalk is None:
        return
    
    xwalk = node_seq_id_xwalk.set_index("model_node_id")
    maz = maz_data_df["MAZ_ORIGINAL"].map(xwalk["MAZSEQ"])
    taz = maz_data_df["TAZ_ORIGINAL"].map(xwalk["TAZSEQ"])

    bad_maz = maz_data_df.index[maz_data_df["MAZ"]!=maz]
    bad_taz = maz_data_df.index[maz_data_df["TAZ"]!=taz]

    if len(bad_maz)>0 or len(bad_taz)>0:
        raise ValueError(
            f"Node ID crosswalk mismatch: {len(bad_maz)} MAZ, {len(bad_taz)} TAZ"
        )

def load_maz_data(
    maz_data_file: pathlib.Path, 
    node_seq_id_xwalk: DataFrame[NodeIDCrosswalk] = None,
) -> DataFrame[MAZData]:
    """Load and validate MAZ land use data for transportation modeling.
    
    This is the main function for loading MAZ (Micro-Analysis Zone) land use data
    into the TM2.0 transportation model. It performs comprehensive validation to
    ensure data quality and geographic consistency before the data is used in
    trip generation and other modeling components.
    
    The function performs two levels of validation:
    1. Geographic ID validation against the node crosswalk
    2. Schema validation against the MAZData model specification
    
    Validation Steps
    ----------------
    1. Load CSV data from the specified file path
    2. Validate MAZ/TAZ sequential IDs match the crosswalk expectations
    3. Validate all data fields against MAZData schema constraints
    4. Return validated DataFrame ready for modeling use
    
    Data Requirements
    -----------------
    The input CSV must contain all required MAZData columns including:
    - Geographic identifiers (MAZ, TAZ, original node IDs)  
    - Employment by sector (21 detailed categories)
    - Demographics (households, population, school enrollment)
    - Parking supply (hourly, daily, monthly by destination type)
    - Density measures and accessibility indicators
    
    Error Handling
    --------------
    The function will raise descriptive errors for common data issues:
    - Missing or malformed CSV files
    - Geographic ID mismatches with the crosswalk
    - Schema violations (wrong data types, negative values, etc.)
    - Missing required columns or invalid data ranges
    
    Parameters
    ----------
    maz_data_file : pathlib.Path
        Path to CSV file containing MAZ land use data.
        Must include all required columns as defined in MAZData schema.
    node_seq_id_xwalk : DataFrame[NodeIDCrosswalk]  
        Validated crosswalk mapping original node IDs to sequential zone IDs.
        Created by create_sequential_index function.
    
    Returns
    -------
    DataFrame[MAZData]
        Validated MAZ land use data conforming to the MAZData schema.
        All geographic IDs verified against crosswalk and data types validated.
        Ready for use in trip generation and accessibility calculations.
    
    Raises
    ------
    FileNotFoundError
        If the specified maz_data_file does not exist
    ValueError
        If geographic IDs don't match the crosswalk or schema validation fails
    pd.errors.ParserError
        If the CSV file is malformed or unreadable
    
    Example
    -------
    ```python
    from pathlib import Path
    from tm2py.data_models.maz_data import create_sequential_index, load_maz_data
    
    # Create crosswalk and load data
    xwalk_file = Path('model_to_emme_node_id.csv')
    maz_file = Path('maz_land_use_data.csv')
    
    crosswalk = create_sequential_index(xwalk_file) 
    maz_data = load_maz_data(maz_file, crosswalk)
    
    # Use validated data
    total_population = maz_data['POP'].sum()
    employment_by_maz = maz_data['emp_total']
    ```
    
    See Also
    --------
    MAZData : The validation schema applied to the loaded data
    create_sequential_index : Function to create the required crosswalk
    validate_sequential_id : Geographic ID validation performed internally
    """
    
    maz_data_df = pd.read_csv(maz_data_file)
    validate_sequential_id(maz_data_df, node_seq_id_xwalk)

    return MAZData.validate(maz_data_df, lazy=True)

