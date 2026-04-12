"""
src.data_platform.core.utils.filesystem

Purpose:
    - common file read and write operations across all source systems

Exposed API:
    - `read_configs()` - given `source_system`, `config_type` and `name`, read and return the config file
    - `ensure_dir()` - ensures path exists, create if not
    - `atomic_write()` - generic atomic method for bytes
"""


from importlib.resources import files
import json
import os
import uuid 
from pathlib import Path

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

def ensure_dir(path: Path|str) -> None:
    """
    Purpose:
        - ensure the path exists
    """
    if isinstance(path, str): path=Path(path)
    path.mkdir(parents=True, exist_ok=True)

def _atomic_replace(src: Path, dst: Path) -> None:
    """
    Purpose:
        - atomically replace temporary file to actual file
    """
    os.replace(src, dst)

def atomic_write_bytes(dst: Path, data: bytes) -> None:
    """
    Purpose:
        - atomically write bytes to `dst`
        - write a temp file in the same directory, fsyncing it, then replacing the destination
    Input:
        - `dst`: destination path (including the filename at the end)
        - `data`: serialized bytes
    """
    ensure_dir(dst.parents)
    tmp = dst.with_name(f".{dst.name}.{uuid.uuid4().hex}.tmp")

    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        _atomic_replace(src=tmp, dst=dst)
    except Exception as e:
        raise RuntimeError(f"Atomic writes failed for {dst}") from e 
    finally:    # clean up
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass    # silent clean up error


# for testing purposes
if __name__ == "__main__":
    print(read_configs(source_system="qbo", config_type="io", name="path.json"))