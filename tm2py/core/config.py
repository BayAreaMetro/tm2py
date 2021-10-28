"""Temporary config implementation with no validation
"""

from types import SimpleNamespace
import toml as _toml
from typing import List, Union


class Configuration:
    """Temporary configuration object to wrap arbitrary TOML config with no validation.

    Provides a dictionary-like interface as well as object properties for accessing
    the settings in a config. Once the config schema is established this will be
    replaced by an object with a defined interface.

    """

    def __init__(self, path: Union[str, List[str]] = None):
        """Load config from toml file(s) at path.

        Args:
            path: valid path to a toml file, or list of paths
        """
        super().__init__()
        if path is not None:
            if isinstance(path, list):
                config = {}
                for path_item in path:
                    next_config = self._load(path_item)
                    _merge_dicts(config, next_config)
                self._set_config(config)
            else:
                self._set_config(self._load(path))

    def __repr__(self):
        items = (f"{k}={v!r}" for k, v in self.__dict__.items())
        return "{}({})".format(type(self).__name__, ", ".join(items))

    def __getitem__(self, key):
        return getattr(self, key)

    @staticmethod
    def _load(path):
        with open(path, "r") as toml_file:
            return _toml.load(toml_file)

    def _set_config(self, data):
        config = {}
        for key, value in data.items():
            config[key] = _dict_to_config(value)
        self.__dict__ = config

    def save(self, path: str):
        """Save config to toml file at path"""
        data = {}
        for key, value in self.__dict__.items():
            data[key] = _config_to_dict(value)
        with open(path, "w") as toml_file:
            _toml.dump(data, toml_file)


class ConfigItem(SimpleNamespace):
    """Support use of both .X and ["X"] from configuration"""

    def __getitem__(self, key):
        return getattr(self, key)

    def items(self):
        """D.items() -> a set-like object providing a view on D's items"""
        return self.__dict__.items()

    def get(self, key, default=None):
        """Return the value for key if key is in the dictionary, else default."""
        return self.__dict__.get(key, default)


__banned_keys = ["items", "get"]


def _dict_to_config(data):
    """Deep copy converting dictionary / list to ConfigItem."""
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            if key in __banned_keys:
                raise Exception(f"name '{key}' is not allowed in config")
            result[key] = _dict_to_config(value)
        return ConfigItem(**result)
    if isinstance(data, list):
        return [_dict_to_config(value) for value in data]
    return data


def _config_to_dict(data):
    """Deep copy converting ConfigItem to dictionary / list."""
    if isinstance(data, ConfigItem):
        result = {}
        for key, value in data.__dict__.items():
            result[key] = _config_to_dict(value)
        return result
    if isinstance(data, list):
        return [_config_to_dict(value) for value in data]
    return data


def _merge_dicts(right, left, path=None):
    """Merges the contents of nested dict left into nested dict right, raising errors in case of namespace conflicts.
    """
    if path is None:
        path = []
    for key in left:
        if key in right:
            if isinstance(right[key], dict) and isinstance(left[key], dict):
                _merge_dicts(right[key], left[key], path + [str(key)])
            else:
                raise Exception('Conflict during merge %s' % '.'.join(path + [str(key)]))
        else:
            right[key] = left[key]
