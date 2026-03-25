"""
src.data_platform.sources.qbo.transformation.col_discovery

Purpose:
    - discover column names across all reports to compose a column superset as Schema for the silver transformation for the nested reports

Exposed API:
    - `compose_column_superset()` - given root path with sub-directory structured as `root/{company_code}/{year_month}.json`, iterate through files to compose columns superset
    - `extract_column_meta()` - extract column meta data for one file object
    - `resolve_json_path()` - compose exact file path to JSON files

Note:
    - assuming column meta data is stored in `obj["Columns"]["Column"]`, error if this assumption fails
"""

from pathlib import Path
import orjson

from data_platform.core.utils.filesystem import read_configs
from data_platform.sources.qbo.utils.contracts import TaskRecord


def resolve_json_path(company_code:str, start:str, raw_path:Path|str) -> Path:
    """
    Purpose:
        - input `company_code` and `start` (e.g., '2025-10-01')
        - compose file_path to bronze JSON storage
    """
    if isinstance(raw_path, str): raw_path = Path(raw_path)
    date_split = start.split("-")
    file_name = f"{date_split[0]}_{int(date_split[1])}.json"
    return raw_path / f"{company_code}" / file_name


def extract_column_meta(obj:dict) -> list[str]:
    """
    Purpose:
        - input the dict object from reading JSON raw file
        - extract and return the column names for the file
        - error if column meta data is missing
    """
    try:
        meta = obj["Columns"]["Column"]
    except Exception as e:
        raise KeyError("obj['Columns']['Column'] missing from PL JSON file") from e
    cols = [item["ColTitle"].replace(" ", "_").replace("/", "_").lower() for item in meta]
    return cols

def _discover_columns(tasks: list[TaskRecord], raw_path:Path) -> set[str]:
    """
    Purpose:
        - go through all related files and compose a superset for expected columns
        - returns `set` object
    """
    columns = set()
    for t in tasks:
        file_path = resolve_json_path(company_code = t["company"], start=t["start"], raw_path=raw_path)
        if file_path.exists():
            with open(file_path, "rb") as f:
                raw = f.read()
            obj = orjson.loads(raw)
            cols = extract_column_meta(obj=obj)
            columns.update(cols)
    return columns

def _add_id_columns(columns: set[str]) -> list[str]:
    """
    Purpose:
        - input column superset (as `set` object), add `*_id` column names, excluding columns from external blacklist
    """
    data_structure_config = read_configs(source_system="qbo", config_type="assumptions", name="data_structure.json")
    try:
        blacklist = data_structure_config["silver"]["nested_reports"]["no_id_cols"]
    except:
        print("WARNING: qbo-json_configs-assumptions-data_structure.json doesn't have ['silver']['nested_reports']['no_id_cols'] for accessing blacklist")
        blacklist = []
    blacklist = set(blacklist)
    id_columns = {
        f"{col}_id" 
        for col in (columns - blacklist)
        if not col.endswith("_id")
    }
    final_columns = columns | id_columns
    return list(sorted(final_columns))

def compose_column_superset(tasks: list[TaskRecord], raw_path:Path|str) -> list[str]:
    """
    Purpose:
        - given a list of `task` and `raw_path` 
        - iterate through all files involved in the task
        - compose a superset for all columns contained in the job
        - including adding `acc` columns and `*_id` columns
    """
    if isinstance(raw_path,str): raw_path = Path(raw_path)
    columns = _discover_columns(tasks=tasks, raw_path=raw_path)
    final_columns = _add_id_columns(columns=columns)
    final_columns = ["acc_id","acc_full"] + final_columns
    return final_columns
