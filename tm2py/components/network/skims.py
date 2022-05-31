"""General skim-related tools."""

from typing import TYPE_CHECKING, Mapping

from numpy import array as NumpyArray

from tm2py.emme.matrix import OMXManager

if TYPE_CHECKING:
    from tm2py.controller import RunController


def get_omx_skim_as_numpy(
    controller: "RunController",
    mode: str,
    time_period: str,
    property: str = "time",
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

    if mode in controller._component_map["highway"].hwy_classes:
        _config = controller.config.highway
        _mode_config = controller._component_map["highway"].hwy_class_configs[mode]

    else:
        raise NotImplementedError("Haven't implemented non highway skim access")

    if property not in _mode_config["skims"]:
        raise ValueError(
            f"Property {property} not an available skim in mode {mode}.\
            Available skims are:  {_mode_config['skims']}"
        )

    # TODO figure out how to get upper() and lower() into actual format string
    _filename = _config.output_skim_filename_tmpl.format(
        time_period=time_period.lower()
    )
    _filepath = controller.run_dir / _config.output_skim_path / _filename
    _matrix_name = _config.output_skim_matrixname_tmpl.format(
        time_period=time_period.lower(),
        mode=mode,
        property=property,
    )

    with OMXManager(_filepath, "r") as _f:
        return _f.read(_matrix_name)


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
            get_omx_skim_as_numpy(controller, mode, _tp, property) * _multiplier
        )

    _blended_time = sum(_scaled_times)
    return _blended_time


## TODO move availability mask from toll choice to here
