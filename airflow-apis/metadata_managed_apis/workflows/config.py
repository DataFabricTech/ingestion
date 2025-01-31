import json
import os
from pathlib import Path


class ConfigurationError(Exception):
    """A configuration error has happened"""


def load_config_file(config_file: Path) -> dict:
    if not config_file.is_file():
        raise ConfigurationError(f"Cannot open config file {config_file}")

    with config_file.open() as raw_config_file:
        raw_config = raw_config_file.read()

    expanded_config_file = os.path.expandvars(raw_config)
    config = json.loads(expanded_config_file)

    return config
