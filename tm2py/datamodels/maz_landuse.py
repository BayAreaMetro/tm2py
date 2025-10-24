import pathlib

import pandas as pd
import numpy as np
import pandera.pandas as pa

from pandera import Field
from pandera.typing import Series, DataFrame


# TODO:
DENSITY_COLUMNS = ['EmpDen','DUDen','PopDen','IntDenBin','EmpDenBin','DuDenBin','PopEmpDenPerMi']
SEQ_INDEX_COLUMNS = ['MAZ','TAZ']

class MAZLandUse(pa.DataFrameModel):
    """
    TODO: add docstring
    Datamodel used to validate if maz landuse input is in correct format and types

    Attributes:
        MAZ (int): MAZ sequential index
        TAZ (int): TAZ sequential index
        MAZ_ORIGINAL (int): MAZ node ID
        TAZ_ORIGINAL (int): TAZ node ID
        DistID (int): district ID
        DistName (str): district name
        CountyID (int): county ID
        CountyName (str): county name
        ACRES (float): zone area in acres
        HH (int): number of households
        POP (int): number of population
        ag (int): Agriculture employment
        art_rec (int): Arts & recreational employment
        constr (int): Construction employment
        eat (int): Eating out employment
        ed_high (int): Higher education employment
        ed_k12 (int): K-12 education employment
        ed_oth (int): Other education employment
        fire (int): Financial, Insurance, real estate employment
        gov (int): Government employment
        health (int): Health employment
        hotel (int): Hotel employment
        info (int): Information employment
        lease (int): Leasing employment
        logis (int): Logistics employment
        man_bio (int): Biological manufacturing employment
        man_lgt (int): Heavy manufacturing employment
        man_hvy (int): Light manufacturing employment
        man_tech (int): Technology manufacturing employment
        natres (int): Natural resources employment
        prof (int): Professional employment
        ret_loc (int): Local retail employment
        ret_reg (int): Regional retail employment
        serv_bus (int): Business services employment
        serv_pers (int): Personal services employment
        serv_soc (int): Social services employment
        transp (int): Transportation employment
        util (int): Utilities employment
        emp_total (int); Total employment
        publicEnrollGradeKto8 (int): Public enrollment grades k-8
        privateEnrollGradeKto8 (int): Private enrollment grades k-8
        publicEnrollGrade9to12 (int): Public enrollment grades 9-12
        privateEnrollGrade9to12 (int): Private enrollment grades 9-12
        comm_coll_enroll (int): Community college enrollment
        EnrollGradeKto8 (int): enrollment grades k-8
        EnrollGrade9to12 (float): enrollment grades 9-12
        collegeEnroll (float): college enrollment
        otherCollegeEnroll (float): other college enrollment
        AdultSchEnrl (int): adult school enrollment 
        hstallsoth (float): Number of hourly stalls, different mgra
        hstallssam (float): Number of hourly stalls, same mgra
        dstallsoth (float): Number of daily stalls, different mgra
        dstallssam (float): Number of daily stalls, same mgra
        mstallsoth (float): Number of monthly stalls, different mgra
        mstallssam (float): Number of monthly stalls, same mgra
        park_area (float): Area of parks in sq. meters
        hparkcost (float): hourly parking cost
        numfreehrs (float): number of free hours
        dparkcost (float): daily parking cost
        mparkcost (float): monthly parking cost
        ech_dist (int):
        hch_dist (int):
        parkarea (int):
        TERMINAL (float):
        MAZ_X (float):
        MAZ_Y (float):
        TotInt (float): Intersection count in 1/2 mile radius of household MGRA from 4D file
        EmpDen (float): employement density
        RetEmpDen (float): Retail Trade Employment Density in 1/2 mile radius of household MGRA from 4D file
        DUDen (float): Dwelling density
        PopDen (float): Population Density in 1/2 mile radius of household MGRA from 4D file
        IntDenBin (int):
        EmpDenBin (int): employment density category (1,2,3) of workplace
        DUDenBin (int): mix category (1,2,3,4) of workplace
        PopEmpDenPerMi (float):
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


def create_sequential_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create stable sequential IDs for MAZ and TAZ nodes.

    Args:
        df: input maz landuse

    Returns:
        maz landuse with two additional sequential index columns: MAZ and TAZ
    """
    df = df.drop(columns=SEQ_INDEX_COLUMNS, errors="ignore")
    
    # MAZ sequential index
    maz_ordered = df["MAZ_ORIGINAL"].sort_values(kind="stable")
    maz_ordered.reset_index(drop=True, inplace=True)
    maz_map = pd.DataFrame({"MAZ_ORIGINAL": maz_ordered, "MAZ": np.arange(1, len(maz_ordered) + 1)})
    
    # TAZ sequential index
    taz_ordered = df["TAZ_ORIGINAL"].drop_duplicates().sort_values(kind="stable")
    taz_ordered.reset_index(drop=True, inplace=True)
    taz_map = pd.DataFrame({"TAZ_ORIGINAL": taz_ordered, "TAZ": np.arange(1, len(taz_ordered) + 1)})

    df = df.merge(maz_map, on="MAZ_ORIGINAL", how="left").merge(
        taz_map, on="TAZ_ORIGINAL", how="left"
    )
    return df

def add_variables(df: pd.DataFrame) -> pd.DataFrame:
    """
    TODO: add docstring
    """
    # maz_data_df = maz_data_df.drop(columns=DENSITY_COLUMNS, errors="ignore")
    # calculate densities
    # maz_data_df["EmpDen"] = maz_data_df["emp_total"]/maz_data_df["ACRES"]

    return df

def load_maz_data(maz_data_file: pathlib.Path) -> DataFrame[MAZLandUse]:
    """
    load MAZ landuse data, create MAZ and TAZ sequential IDs,
    add additional variables, and validate against the MAZLandUse schema.

    Args:
        maz_data_file: path to maz landuse data

    Returns:
        Validated dataframe with MAZ and TAZ sequential IDs and any calculated variables.
    """
    maz_data_df = pd.read_csv(maz_data_file)
    maz_data_df = create_sequential_index(maz_data_df)
    maz_data_df = add_variables(maz_data_df)

    return MAZLandUse.validate(maz_data_df, lazy=True)

