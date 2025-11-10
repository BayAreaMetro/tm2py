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
        EnrollGradeKto8 (int): Total enrollment grades k-8
        EnrollGrade9to12 (float): Total enrollment grades 9-12
        collegeEnroll (float): Major College enrollment
        otherCollegeEnroll (float): Other College enrollment
        AdultSchEnrl (int): Adult School enrollment 
        hstallsoth (float): Number of stalls allowing hourly parking for trips with destinations in other MAZs
        hstallssam (float): Number of stalls allowing hourly parking for trips with destinations in the same MAZ
        dstallsoth (float): Stalls allowing daily parking for trips with destinations in other MAZs
        dstallssam (float): Stalls allowing daily parking for trips with destinations in the same MAZ
        mstallsoth (float): Stalls allowing monthly parking for trips with destinations in other MAZs
        mstallssam (float): Stalls allowing monthly parking for trips with destinations in the same MAZ
        park_area (float): Area of parks in sq. meters
        hparkcost (float): Average cost of parking for one hour in hourly stalls in this MAZ, dollars
        numfreehrs (float): Number of hours of free parking allowed before parking charges begin in hourly stalls
        dparkcost (float): Average cost of parking for one day in daily stalls, dollars
        mparkcost (float): Average cost of parking for one day in monthly stalls, amortized over 22 workdays, dollars
        ech_dist (int): Elementary school district
        hch_dist (int): High school district
        parkarea (int): parking area (1 through 4)
        TERMINAL (float):
        MAZ_X (float):
        MAZ_Y (float):
        TotInt (float): Total intersections within 1/2 mile of MAZ
        EmpDen (float): Employment per acre within 1/2 mile of MAZ
        RetEmpDen (float): Retail employment per acre within 1/2 mile of MAZ
        DUDen (float): Households per acre within 1/2 mile of MAZ
        PopDen (float): Population per acre within 1/2 mile of MAZ
        IntDenBin (int): Intersection density bin (1 through 3 where 3 is the highest)
        EmpDenBin (int): Employment density bin (1 through 3 where 3 is the highest)
        DUDenBin (int): Houseold density bin (1 through 3 where 3 is the highest)
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

class NodeIDCrosswalk(pa.DataFrameModel):
    """
    Datamodel used to validate node ID crosswalk

    Attributes:
        model_node_id (int): model node ID
        MAZSEQ (int): MAZ sequential index
        TAZSEQ (int): TAZ sequential index
        EXTSEQ (int): External sequential index
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
    """
    Create stable sequential IDs for MAZ and TAZ nodes.

    Args:
        model_to_emme_node_id_xwalk: model ID to Emme node ID crosswalk 
        written out by Lasso in the network build process

    Returns:
        crosswalk file of model ID to TAZ, MAZ, and external TAZ sequential ID.
        Will be further used to validate the TAZ, MAZ columns in maz data input.   
    """
    node_id_df = pd.read_csv(model_to_emme_node_id_xwalk)
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
    node_seq_id_xwalk: DataFrame[NodeIDCrosswalk]
) -> None:
    """
    Validate the TAZ, MAZ columns in maz data input

    Args:
        maz_data_df: maz landuse input
        node_seq_id_xwalk: validated node ID to sequential ID crosswalk

    Return:
        None
        Fail if any node ID mismatch
    """
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
    node_seq_id_xwalk: DataFrame[NodeIDCrosswalk]
) -> DataFrame[MAZData]:
    """
    load MAZ landuse data, validate the TAZ and MAZ IDs,
    validate against the MAZData schema.

    Args:
        maz_data_file: path to maz landuse data
        node_seq_id_xwalk: validated node ID to sequential ID crosswalk

    Returns:
        Validated maz data.
    """
    
    maz_data_df = pd.read_csv(maz_data_file)
    validate_sequential_id(maz_data_df, node_seq_id_xwalk)

    return MAZData.validate(maz_data_df, lazy=True)

