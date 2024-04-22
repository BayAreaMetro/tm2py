"""General skim-related tools."""

import itertools
import os
from typing import TYPE_CHECKING, Collection, Mapping, Union

import numpy as np
from numpy import array as NumpyArray

from tm2py.emme.matrix import OMXManager

if TYPE_CHECKING:
    from tm2py.controller import RunController


def get_summed_skims(
    controller: "RunController",
    mode: Union[str, Collection[str]],
    veh_group_name: str,
    time_period: str,
    property: Union[str, Collection[str]],
    omx_manager: OMXManager = None,
) -> NumpyArray:
    """Sum skim matrices for list of properties and modes for time period.

    Args:
        controller (RunController): _description_
        mode (Union[str,Collection[str]]): _description_
        time_period (str): _description_
        property (Union[str,Collection[str]]): _description_
        omx_manager (OMXManager, optional): _description_. Defaults to None.

    Returns:
        NumpyArray: Numpy matrix of sums of skims from list.
    """

    if isinstance(mode, str):
        mode = [mode]
    if isinstance(property, str):
        property = [property]

    _mode_prop = itertools.product(mode, property)

    _mx_list = [
        get_omx_skim_as_numpy(
            controller, mode, veh_group_name, time_period, prop, omx_manager
        )
        for mode, prop in _mode_prop
    ]

    if len(_mx_list) == 1:
        return _mx_list[0]

    return np.add(*_mx_list)


def get_omx_skim_as_numpy(
    controller: "RunController",
    skim_mode: str,
    veh_group_name: str,
    time_period: str,
    property: str = "time",
    omx_manager: OMXManager = None,
) -> NumpyArray:
    """Get OMX skim by time and mode from folder and return a zone-to-zone NumpyArray.

    TODO make this independent of a model run (controller) so can be a function to use
    in analysis.

    Args:
        controller: tm2py controller, for accessing config.
        mode: Mode to get.
        time_period: Time period to get.
        property: Property to get. Defaults to "time".
    """

    if time_period.upper() not in controller.time_period_names:
        raise ValueError(
            f"Skim time period {time_period.upper()} must be a subset of config time periods: {controller.time_period_names}"
        )

    # TODO need to more dutifully map skim modes to network modes
    _hwy_classes = {c.name: c for c in controller.config.highway.classes}
    if skim_mode in _hwy_classes.keys():
        _config = controller.config.highway
        _mode_config = _hwy_classes[skim_mode]

    else:
        raise NotImplementedError("Haven't implemented non highway skim access")

    if property not in _mode_config["skims"]:
        property = property + "_" + veh_group_name
        if property not in _mode_config["skims"]:
            raise ValueError(
                f"Property {property} not an available skim in mode {skim_mode}.\
                Available skims are:  {_mode_config['skims']}"
            )

    _matrix_name = _config.output_skim_matrixname_tmpl.format(
        time_period=time_period.upper(),
        mode=skim_mode,
        property=property,
    )

    # TODO figure out how to get upper() and lower() into actual format string
    if omx_manager is None:
        _filename = _config.output_skim_filename_tmpl.format(
            time_period=time_period.lower()
        )
        _filepath = controller.run_dir / _config.output_skim_path / _filename
        with OMXManager(_filepath, "r") as _f:
            omx_data = _f.read(_matrix_name)
            _f.close()
            return omx_data
    else:
        _filename = _config.output_skim_filename_tmpl.format(
            time_period=time_period.lower()
        )
        if os.path.basename(omx_manager._file_path) != _filename:
            omx_manager.close()
            omx_manager._file_path = (
                controller.run_dir / _config.output_skim_path / _filename
            )
        omx_manager.open()
        omx_data = omx_manager.read(_matrix_name)
        omx_manager.close()
        return omx_data


def get_blended_skim(
    controller: "RunController",
    mode: str,
    property: str = "time",
    blend: Mapping[str, float] = {"AM": 0.3333333333, "MD": 0.6666666667},
) -> NumpyArray:
    r"""Blend skim values for distribution calculations.

    Note: Cube outputs skims\COM_HWYSKIMAM_taz.tpp, r'skims\COM_HWYSKIMMD_taz.tpp'
    are in the highway_skims_{period}.omx files in Emme version
    with updated matrix names, {period}_trk_time, {period}_lrgtrk_time.
    Also, there will no longer be separate very small, small and medium
    truck times, as they are assigned together as the same class.
    There is only the trk_time.

    Args:
        controller: Emme controller.
        mode: Mode to blend.
        property: Property to blend. Defaults to "time".
        blend: Blend factors, a dictionary of mode:blend-multiplier where:
            - sum of all blend multpiliers should equal 1. Defaults to `{"AM":1./3, "MD":2./3}`
            - keys should be subset of _config.time_periods.names
    """

    if sum(blend.values()) != 1.0:
        raise ValueError(f"Blend values must sum to 1.0: {blend}")

    _scaled_times = []
    for _tp, _multiplier in blend.items():
        _scaled_times.append(
            get_omx_skim_as_numpy(controller, mode, "", _tp, property) * _multiplier
        )

    _blended_time = sum(_scaled_times)
    return _blended_time


## TODO move availability mask from toll choice to here
