"""Temporary config implementation with no validation
"""

from types import SimpleNamespace
import toml

__BANNED_KEYS = ["items", "get"]


class Configuration:
    """Temporary configuration object to wrap arbitrary TOML config with no validation"""

    def __init__(self, path: str = None):
        self.load(path)
        self.load(self.model.config)

    def __repr__(self) -> str:
        items = (f"{k}={v!r}" for k, v in self.__dict__.items())
        return "{}({})".format(type(self).__name__, ", ".join(items))

    def load(self, path: str) -> None:
        """Load config from toml file at path"""
        with open(path, "r") as toml_file:
            data = toml.load(toml_file)
        config = {}
        for key, value in data.items():
            config[key] = _dict_to_config(value)
        self.__dict__.update(config)

    def save(self, path: str) -> None:
        """Save config to toml file at path"""
        data = {}
        for key, value in self.__dict__.items():
            data[key] = _config_to_dict(value)
        with open(path, "w") as toml_file:
            toml.dump(data, toml_file)


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


def _dict_to_config(data):
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            if key in __BANNED_KEYS:
                raise Exception(f"name '{key}' is not allowed in config")
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
