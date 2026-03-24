"""
src.data_platform.sources.qbo.transformation.col_discovery

Purpose:
    - discover column names across all reports to compose a column superset as Schema for the silver transformation for the nested reports

Exposed API:
    - `discover()` - given root path with sub-directory structured as `root/{company_code}/{year_month}.json`, iterate through files to compose columns superset
    - `extract_column_meta()` - extract column meta data for one file object
    - `resolve_json_path` - compose exact file path to JSON files

Note:
    - assuming column meta data is stored in `obj["Columns"]["Column"]`, error if this assumption fails
"""

from pathlib import Path


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

