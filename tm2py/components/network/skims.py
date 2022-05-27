"""General skim-related tools."""

from typing import TYPE_CHECKING, Mapping

from numpy import array as NumpyArray

from tm2py.emme.matrix import OMXManager

if TYPE_CHECKING:
    from tm2py.controller import RunController


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
    _config = controller.config
    _skim_path_tmplt = controller.get_abs_path(_config.highway.output_skim_path)

    if not set(blend.keys()).issubset(_tp.name.upper() for _tp in _config.time_periods):
        raise ValueError(
            f"Blend keys must be a subset of time periods: {_config.time_periods}"
        )

    if sum(blend.values()) != 1.0:
        raise ValueError(f"Blend values must sum to 1.0: {blend}")

    if mode not in _config.highway.modes:
        raise ValueError(f"Mode must be one of: {_config.highway.modes}")

    _scaled_times = []
    for _tp, _multiplier in blend.items():
        with OMXManager(_skim_path_tmplt.format(period=_tp.upper()), "r") as _f:
            _scaled_times.append(
                _f.read(f"{_tp.lower()}_{mode}_{property}") * _multiplier
            )

    # TODO if OMXManager is just reading OMX files, we should just do that directly. If it needs
    # to access EmmeBank then can stay.

    _blended_time = sum(_scaled_times)
    return _blended_time


def _availability_mask(
    controller: "RunController",
    mode: str = None,
    timeperiod: str = None,
    mask=[("cost", ">", 9999), ("time", "<=", 0)],
):
    """TODO - this is not implemented.

    TODO, masking is currently in emme.matrix.mask_non_available but needs to be more generic

    Args:
        controller (RunController): _description_
        mode (str, optional): _description_. Defaults to None.
        timeperiod (str, optional): _description_. Defaults to None.
        mask (list, optional): _description_. Defaults to [("cost", ">", 9999), ("time", "<=", 0)].

    Returns:
        _type_: _description_
    """
    raise NotImplementedError
    _config = controller.config
    _skim_path_tmplt = controller.get_abs_path(_config.highway.output_skim_path)
    _masks = []
    with OMXManager(_skim_path_tmplt.format(period=timeperiod.upper()), "r") as _f:
        for _prop, _op, _val in mask:
            _skim = _f.read(f"{timeperiod.lower()}_{mode}_{_prop}")
            _masks.append(_skim.__getattribute__(_op)(_val))

    mask = any(mask)
    return mask
