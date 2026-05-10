import yaml
from pathlib import Path
from typing import Dict, Any

def read_config(config_path: str="configs/app_config.yml") -> Dict[str, Any]:
    """
    Reads YAML config file and returns it as a Python Dictionary
    :param config_path:
    :return:
    """
    path =Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with path.open("r") as file:
        config = yaml.safe_load(file)

    if not config:
        raise ValueError(f"config file is empty: {config_path}")
    return config

