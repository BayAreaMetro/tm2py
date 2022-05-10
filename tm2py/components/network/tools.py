"""Tools for the highway network component."""

from typing import Mapping

from numpy import array as NumpyArray


from tm2py import Controller
from tm2py.emme.matrix import OMXManager


def get_blended_skim(
    self, 
    controller: Controller,
    mode: str,
    property: str = "time",
    blend: Mapping[str,float] = {"AM":1./3, "MD":2./3},
    ) -> NumpyArray:
    """Blend skim values for distribution calculations.

    Note: Cube outputs skims\COM_HWYSKIMAM_taz.tpp, skims\COM_HWYSKIMMD_taz.tpp
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
    _config = controller.config
    _skim_path_tmplt = self.controller.get_abs_path(_config.highway.output_skim_path)

    try:
        assert set(blend.keys()).issubset(_tp.name.upper() for _tp in _config.time_periods)
    except AssertionError:
        raise ValueError(f"Blend keys must be a subset of time periods: {_config.time_periods}")

    try:
        assert sum(blend.values()) == 1.0
    except AssertionError:
        raise ValueError(f"Blend values must sum to 1.0: {blend}")

    try:
        assert mode in _config.highway.modes
    except:
        raise ValueError(f"Mode must be one of: {_config.highway.modes}")

    _scaled_times = []
    for _tp, _multiplier in blend.items():
        with OMXManager(_skim_path_tmplt.format(period=_tp.upper()), "r") as _f:
            _scaled_times.append(_f.read(f"{_tp.lower()}_{mode}_{property}")*_multiplier)
    
    # TODO if OMXManager is just reading OMX files, we should just do that directly. If it needs
    # to access EmmeBank then can stay.

    _blended_time = sum(_scaled_times)
