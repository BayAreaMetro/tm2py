"""Temporary config implementation with no validation
"""

from types import SimpleNamespace
import toml as _toml


class Configuration:
    """Temporary configuration object to wrap arbitrary TOML config with no validation"""

    def __init__(self, path: str = None):
        super().__init__()
        if path is not None:
            self.load(path)

    def __repr__(self):
        items = (f"{k}={v!r}" for k, v in self.__dict__.items())
        return "{}({})".format(type(self).__name__, ", ".join(items))

    def load(self, path: str):
        """Load config from toml file at path"""
        with open(path, "r") as toml_file:
            data = _toml.load(toml_file)
        config = {}
        for key, value in data.items():
            config[key] = _dict_to_config(value)
        self.__dict__.update(config)

    def save(self, path: str):
        """Save config to toml file at path"""
        data = {}
        for key, value in self.__dict__.items():
            data[key] = _config_to_dict(value)
        with open(path, "w") as toml_file:
            _toml.dump(data, toml_file)


class ConfigItem(SimpleNamespace):
    """Support use of both .X and ["X"] from configuration"""

    # Skip too-few-public methods recomendation
    # pylint: disable=R0903
    def __getitem__(self, key):
        return getattr(self, key)


def _dict_to_config(data):
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            result[key] = _dict_to_config(value)
        return ConfigItem(**result)
    if isinstance(data, list):
        return [_dict_to_config(value) for value in data]
    return data


def _config_to_dict(data):
    if isinstance(data, ConfigItem):
        result = {}
        for key, value in data.__dict__.items():
            result[key] = _config_to_dict(value)
        return result
    if isinstance(data, list):
        return [_config_to_dict(value) for value in data]
    return data
