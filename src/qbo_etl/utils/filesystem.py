"""
src.qbo_etl.utils.filesystem

Purpose:
    - common file read and write operations

Exposed API:
    - `read_configs()` - given `config_type` and `name`, read and return the config file
"""


from importlib.resources import files
import json

def read_configs(config_type: str, name: str) -> dict:
    path = files("qbo_etl.json_configs").joinpath(f"{config_type}/{name}")
    return json.loads(path.read_text())


# for testing purposes
if __name__ == "__main__":
    print(read_configs(config_type="io", name="path.json"))