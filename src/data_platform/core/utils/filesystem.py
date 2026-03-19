"""
src.data_platform.core.utils.filesystem

Purpose:
    - common file read and write operations across all source systems

Exposed API:
    - `read_configs()` - given `source_system`, `config_type` and `name`, read and return the config file
"""


from importlib.resources import files
import json

def read_configs(source_system: str, config_type: str, name: str) -> dict:
    """
    Purpose:
        - standardized methods for reading config files
    Input:
        - `source_system`: the data source system e.g., "qbo"
        - `config_type`: the config type e.g., "contracts"
        - `name`: the name of the config file e.g., "facts.json"
    """
    p = f"data_platform.sources.{source_system}.json_configs" if source_system.lower() != "core" else f"data_platform.core.json_configs"
    path = files(p).joinpath(f"{config_type}/{name}")
    return json.loads(path.read_text())


# for testing purposes
if __name__ == "__main__":
    print(read_configs(source_system="qbo", config_type="io", name="path.json"))