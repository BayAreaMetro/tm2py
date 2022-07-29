from pathlib import Path
from typing import Collection, Dict, Mapping, Union

import numpy as np
import pandas as pd
import openmatrix as _omx

NumpyArray = np.array


def omx_to_dict(
    omx_filename: Union[str, Path],
    matrices: Union[Mapping[str, str], Collection[str]] = None,
) -> Dict[str, NumpyArray]:
    """Reads OMX file and returns a dictionary matrix names mapped to NumpyArrays.

    Args:
        omx_filename (Union[str,Path]): Filename of OMX file.
        matrices Union[Mapping[str,str],Collection[str]], optional): Either a list of matrix names
            to read or a dictionary mapping the output dictionary key to the matrix name in the
            OMX file to map to it. Defaults to all in file.

    Returns:
        Dict[str,NumpyArray]: _description_
    """

    # if specified as a list, then turn into a dictionary
    if isinstance(matrices, list):
        _matrices = {m: m for m in matrices}
    else:
        _matrices = matrices

    omx_file = _omx.open_file(omx_filename)

    # check to make sure matrices are available in file
    _avail_matrices = omx_file.list_matrices()
    _req_matrices = list(matrices.values())

    if _req_matrices:
        if not set(_req_matrices).issubset(set(_avail_matrices)):
            raise ValueError(
                f"Not all specified matrices ({ _req_matrices}) found in omx file.\
                available matrices: {_avail_matrices}"
            )
    else:
        _matrices = {m: m for m in _avail_matrices}
    
    omx_dict = {key: omx_file[omx_name].read() for key, omx_name in _matrices.items()}
    omx_file.close()
    return omx_dict


def df_to_omx(
    df: pd.DataFrame,
    matrix_dict: Mapping[str, str],
    omx_filename: str,
    orig_column: str = "ORIG",
    dest_column: str = "DEST",
):
    """Export a dataframe to an OMX matrix file.

    Args:
        df (pd.DataFrame): DataFrame to export.
        omx_filename (str): OMX file to write to.
        matrix_dict (Mapping[str, str]): Mapping of OMX matrix name to DF column name.
        orig_column (str, optional): Origin column name. Defaults to "ORIG".
        dest_column (str, optional): Destination column name. Defaults to "DEST".
    """
    df = df.reset_index()

    # Get all used Zone IDs to produce index and zone mapping in OMX file
    zone_ids = sorted(set(df[orig_column]).union(set(df[dest_column])))
    num_zones = len(zone_ids)

    # Map zone id to zone index #
    zone_map = dict((z, i) for i, z in enumerate(zone_ids))

    # calculate omx index of entries in numpy array list
    df["omx_idx"] = df.apply(
        lambda r: zone_map[r[orig_column]] * num_zones + zone_map[r[dest_column]],
        axis=1,
    )

    _omx_file = _omx.open_file(omx_filename, "w")
    _omx_file.create_mapping("zone_number", zone_ids)

    try:
        for _name, _df_col in matrix_dict.items():
            _array = np.zeros(shape=(num_zones, num_zones))
            np.put(
                _array,
                df["omx_idx"].to_numpy(),
                df[_df_col].to_numpy(),
            )

            _omx_file.create_matrix(_name, obj=_array)

            # TODO add logging
    finally:
        _omx_file.close()
