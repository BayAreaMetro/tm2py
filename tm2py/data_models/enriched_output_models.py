"""Enriched Outputs Data Model for TM 2

"""

from pydantic import BaseModel, Field, ValidationError
from typing import Optional
from enum import Enum
import pandas as pd
from tm2py.logger import Logger
import logging

class AutoSufficiency(int, Enum):
    ZERO = 0
    LESS_THAN_WORKERS = 1
    EQUAL_OR_MORE_WORKERS = 2

class TimeOfDay(str, Enum):
    EA = "EA"  # Early AM
    AM = "AM"
    MD = "MD"  # Midday
    PM = "PM"
    EV = "EV"  # Evening



class PopSimHouseholdModel(BaseModel):
    """Validated population sim input household data"""
    HHID: int
    TAZ_NODE: int
    MAZ_NODE: int
    MTCCountyID: int
    HHINCADJ: int
    NWRKRS_ESR: int
    VEH: int
    NP: int
    HHT: int
    BLD: int
    TYPE:  int

class CTRAMPHouseholdModel(BaseModel):
    """Validated CTRAMP output household data"""
    hh_id: int
    HOME_MAZ_SEQ: int
    income: int
    size: int
    workers: int
    autos: int
    automated_vehicles: int
    transponder: int
    pre_et_cdap_pattern: str
    cdap_pattern: str
    jtf_choice: int

class HouseholdModel(PopSimHouseholdModel, CTRAMPHouseholdModel):
    """Validated household data"""
    HHID: Optional[int]
    incQ: str
    autoSuff: AutoSufficiency
    autoSuff_label: str
    MAZ_SEQ: int = Field(..., gt=0)
    TAZ_SEQ: int = Field(..., gt=0)
    kidsNoDr: int = Field(..., ge=0, le=1, description="Binary: 1=has kids no driver")
    
    class Config:
        use_enum_values = True  # Store as raw values, not enum objects

class PopSimPersonModel(BaseModel):
    """Validated population sim input person data"""
    HHID: int = Field(..., gt=0)
    PERID: int = Field(..., gt=0)
    AGEP: int
    SEX: int
    SCHL: int
    OCCP: int
    WKHP: int
    WKW: int
    EMPLOYED: int
    ESR: int
    SCHG: int

    class Confiig:
        use_enum_values = True

class CTRAMPPersonModel(BaseModel):
    """Validated CTRAMP output person data"""
    hh_id: int
    person_id: int
    person_num: int = Field(..., gt=0)
    age: int
    gender: str
    type: str
    value_of_time: int
    transitSubsidy_choice: int
    transitSubsidy_percent: int = Field(..., ge=0, le=1)
    naicsCode: int
    preTelecommuteCdap: str
    telecommute: str
    cdap: str
    imf_choice: int
    inmf_choice: int
    fp_choice: int
    reimb_pct: int
    sampleRate: int
    workDCLogsum: int
    schoolDCLogsum: int

    class Config:
        use_enum_values = True

class PersonModel(PopSimPersonModel, CTRAMPPersonModel):
    """Validated person data"""
    HHID: Optional[int]
    PERID: Optional[int]
    incQ: str
    size: int = Field(..., ge=1, le=20)
    autos: int = Field(..., ge=0)
    kidsNoDr: int = Field(..., ge=0, le=1)
    MAZ_NODE: int
    TAZ_NODE: int
    MTCCountyID: int = Field(..., ge=1, le=9)
    
    class Config:
        use_enum_values = True

class TripModel(BaseModel):
    """Validated trip data"""
    hh_id: int = Field(..., gt=0)
    person_id: int = Field(..., ge=0)
    trip_mode: int = Field(..., ge=1, le=17, description="Mode 1-17")
    trip_mode_label: str
    origin_TAZ_SEQ: int = Field(..., gt=0)
    destination_TAZ_SEQ: int = Field(..., gt=0)
    trip_time: float = Field(..., ge=0, description="Minutes")
    trip_dist: float = Field(..., ge=0, description="Miles")
    trip_cost: float = Field(..., ge=0, description="Dollars")
    incQ: str
    autoSuff: AutoSufficiency
    autoSuff_label: str
    
    class Config:
        use_enum_values = True

class TourModel(BaseModel):
    """Validated tour data"""
    hh_id: int = Field(..., gt=0)
    person_id: int = Field(..., ge=0)
    tour_mode: int = Field(..., ge=1, le=17)
    tour_mode_label: str
    tour_purpose: str = Field(..., description="Work, School, Shopping, etc")
    num_participants: int = Field(..., ge=1)
    tour_time: float = Field(..., ge=0)
    tour_dist: float = Field(..., ge=0)
    incQ: str
    
    class Config:
        use_enum_values = True

class WorkSchoolLocation(BaseModel):
    """Validate work school location data"""
    hh_id: int
    HOME_MAZ_SEQ: int
    HOME_MAZ_NODE: int
    HOME_TAZ_NODE: int
    HOME_DistID: int
    HOME_CountyID: int
    Income: int
    PersonNum: int
    PersonType: int
    PersonAge: int
    EmploymentCategory: int
    StudentCategory: int
    WorkSegment: int
    SchoolSegment: int
    WorkLocation: int
    WORK_MAZ_SEQ: int
    WORK_MAZ_NODE: int
    WORK_TAZ_NODE: int
    WORK_DistID: int
    WORK_CountyID: int
    WorkLocationDistance: int
    WorkLocationLogsum: int
    SchoolLocation: int
    SCHOOL_MAZ_SEQ: int
    SCHOOL_MAZ_NODE: int
    SCHOOL_TAZ_NODE: int
    SCHOOL_DistID: int
    SCHOOL_CountyID: int
    SchoolLocationDistance: int
    SchoolLocationLogsum: int


def validate_dataframe(df: pd.DataFrame, model_class) -> pd.DataFrame:
    """
    Validate DataFrame rows against Pydantic model.
    Returns the original DataFrame if all rows valid, otherwise raises error with details.

    Args:
        df: DataFrame to validate the Pydantic model
        model_class: Model Class

    """
    errors = []
        
    rows = df.to_dict(orient='records')

    for idx, row in enumerate(rows):
        try:
            # Convert row to dict and validate
            model_class(**row)
        except ValidationError as exc:
            errors.append(f"Row {idx}: {str(exc)}")
            if len(errors) >= 10:  # Stop after 10 errors to avoid spam
                errors.append(f"... and {len(df) - 10} more errors")
                break
    
    if errors:
        error_msg = "\n".join(errors)
        
        logging.error(f"Validation failed:\n{error_msg}")
        raise ValueError(f"Data validation failed for {model_class.__name__}")
    
    logging.info(f"Validated {len(df):,} {model_class.__name__} rows")
    return df