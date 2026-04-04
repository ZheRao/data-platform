"""
src.data_platform.sources.qbo.transformation.single_file_traversal

Purpose:
    - ingest raw Bronze nested JSON data structure, convert into tabular format and write out as spark data frame in parquet format

Exposed API:
    - `flatten_one_file()` - given `company`, `start`, `path`, load file, crawl through and yield all data nodes
"""

from typing import Iterator, Dict
import orjson
from pathlib import Path

from data_platform.sources.qbo.transformation.schema_discovery import extract_column_meta, resolve_json_path

def _identify_node_type(node: dict) -> str:
    """
    Purpose:
        - Classify a QBO JSON node into a strict, closed set of structural types
        - Enable deterministic traversal and prevent silent schema drift

    Design Philosophy:
        - Classification is strict and fail-loud
        - Any unexpected structure raises immediately
        - Every node MUST map to exactly one known type

    Node Types:
        - `Data`: leaf node containing transaction records
        - `Summary Only`: terminal summary node with no traversal structure
        - `Category End`: structural marker indicating end of a category branch
        - `Include Data For Parent`: transaction rows belonging to parent account (no header)
        - `Category`: grouping node without account identity
        - `Account`: node with account identity and nested transactions

    Validation Model (Fail-Loud Ladder):
        1. Presence check → required keys must exist → `KeyError`
        2. Type check → values must match expected types → `TypeError`
        3. Content check → values must be non-empty / valid → `ValueError`
        4. Classification closure → must match exactly one node type → `ValueError`

    Structural Rules:

        Prerequisite:
            - `node['type']` must exist and be either "Data" or "Section"

        Data:
            - type == "Data"
            - `ColData` exists, is a non-empty list

        Section nodes:
            - `Summary` must exist

        Summary Only:
            - no `Header`
            - no `Rows`

        Category End:
            - no `Header`
            - `Rows` exists but is empty:
                - {} OR {"Row": []}

        Include Data For Parent:
            - no `Header`
            - `Rows["Row"]` exists and is a non-empty list

        Category / Account:
            - `Header` exists
            - `Header["ColData"]` is non-empty list
            - `Rows["Row"]` exists and is non-empty list

            Distinction:
                - if `Header.ColData[0].id` exists → `Account`
                - else → `Category`

    Failure Behavior:
        - Any deviation from expected structure raises immediately
        - Errors include contextual node summary for debugging

    Rationale:
        - QBO JSON is externally controlled and may change over time
        - Silent tolerance risks data loss or misclassification
        - Therefore classification must be strict and diagnostic
    """
    # node attributes
    node_type = node.get("type", "")
    if node_type == "": raise KeyError(f"node['type'] is missing, and node type cannot be determined - node summary - {node.get('Summary')}")

    # Data Node
    if node_type == "Data":
        if "ColData" not in node: raise KeyError(f"expecting 'ColData' in data nodes, missing 'ColData' - node - {node}")
        data = node.get("ColData", [])
        if not isinstance(data, list): raise TypeError(f"data_node['ColData'] expected to be list, got - {type(data)} - node - {node}")
        if len(data) > 0: return "Data"
        else: raise ValueError(f"data node not expected to be empty - node - {node}")

    if node_type != "Section": raise ValueError(f"node['type'] not in [`Data`, `Section`], examine node - {node}")
    
    node_summary = node.get("Summary", [])
    if not node_summary: raise KeyError(f"node['Summary'] is empty and it shouldn't be, check node - {node}")
    
    if "Header" not in node:
        if "Rows" not in node: return "Summary Only"    # no header, no Rows -> Summary Only
        rows = node.get("Rows")
        if not isinstance(rows, dict): raise TypeError(f"expecting node['Rows'] to be dictionary, find {type(rows)} - node summary - {node_summary}")
        if (rows == {}): 
            return "Category End"    # no header, Empty Rows -> Category End
        if "Row" not in rows:
            raise KeyError(f"expecting node['Rows'] to be empty or contain 'Row' as the key, found non-empty node['Rows'] with keys {rows.keys()}")
        sub_rows = rows.get("Row")
        if not isinstance(sub_rows, list): 
            raise TypeError(f"expected node['Rows']['Row'] to be a list, got - {type(sub_rows)} - node summary - {node_summary}")
        if len(sub_rows) == 0: return "Category End"    # no header, empty ["Rows"]["Row"] -> category end
        return "Include Data For Parent"

    # category or account: with header, with/without ID
    ## header check
    header = node.get("Header")
    if not isinstance(header, dict): raise TypeError(f"expected node['Header'] to be dict, got - {type(header)} - node summary - {node_summary}")
    if "ColData" not in header: raise KeyError(f"expecting 'ColData' inside 'Header', missing 'ColData' - node summary - {node_summary}")
    coldata = header.get("ColData")
    if not isinstance(coldata, list): raise TypeError(f"expecting node['Header']['ColData'] to be list, got - {type(coldata)} - node summary - {node_summary}")
    if len(coldata) == 0: raise ValueError(f"unknown node type (header, no ColData for account/category inference) - node summary - {node_summary}")
    ## nested data check
    if "Rows" not in node: raise KeyError(f"for category/account node with header, expecting 'Rows' inside node, missing - node summary - {node_summary}")
    rows = node.get("Rows")
    if not isinstance(rows, dict): raise TypeError(f"expecting node['Rows'] to be dictionary, find {type(rows)} - node summary - {node_summary}")
    if "Row" not in rows:
        raise KeyError(f"for category/account node with header, expecting 'Row' inside node['Rows'], missing - node summary - {node_summary}")
    sub_rows = rows.get("Row")
    if not isinstance(sub_rows, list): 
        raise TypeError(f"expected node['Rows']['Row'] to be a list, got - {type(sub_rows)} - node summary - {node_summary}")
    if len(sub_rows) == 0: raise ValueError(f"expected data contents to be in category/account nodes, empty node['Rows']['Row'] - node summary - {node_summary}")
    
    # check whether it is Account or Category node
    header_meta = coldata[0]
    if not isinstance(header_meta, dict): raise TypeError(f"expected elements in list of node['Header']['ColData'] to be dict, got {type(header_meta)} - node summary - {node_summary}")
    idx = header_meta.get("id", "")
    if idx: return "Account"
    else: return "Category"


def _extract_data_node(node: dict, columns: list[str], acc_info:dict[str,str], company_info:str) -> dict:
    """
    Purpose:
        - given a data node and raw column names, extract actual record as a dictionary
    """
    records = dict.fromkeys(columns, "")
    raw_records = node.get("ColData", "")
    if raw_records == "": raise ValueError("Empty Data node")
    for i, item in enumerate(raw_records):
        column_name = columns[i]
        column_value = item.get("value", "")
        records[column_name] = column_value
        column_id = item.get("id", "")
        if column_id:
            records[column_name+"_id"] = column_id
    for k, v in acc_info.items():
        records[k] = v
    records["corp"] = company_info
    return records

def _crawler(node:dict, columns: list[str],  company_info:str, acc_info:dict[str,str]|None = None) -> Iterator[Dict[str,str]]:
    """
    Purpose:
        - generator that `yield` leaf node data records from crawling through all sub nodes of current node
    Input:
        - `node`: nested dictionary from raw data from QBO API
        - `columns`: expected columns, should be extracted directly from report meta data
        - `company_info`: company code
        - `acc_info`: account info carried to consolidate with leaf node transaction, should not be provided, it would be overwritten anyway
    """
    node_type = _identify_node_type(node=node)
    # for Category End or Summary Only node, end here
    if node_type in ["Category End","Summary Only"]:
        return
    # for Category node, just continue with sub nodes or end if no sub nodes
    elif node_type in ["Category", "Include Data For Parent"]:
        if "Rows" in node and "Row" in node["Rows"]:
            for sub_node in node["Rows"]["Row"]:
                yield from _crawler(node=sub_node, columns=columns, acc_info=acc_info, company_info=company_info)
        else: 
            return
    # for Account node, extract info and continue
    elif node_type == "Account":
        acc_data = node["Header"]["ColData"][0]
        acc_info = {"acc_full": acc_data["value"], "acc_id": acc_data["id"]}
        if "Rows" in node and "Row" in node["Rows"]:
            for sub_node in node["Rows"]["Row"]:
                yield from _crawler(node=sub_node, columns=columns, acc_info=acc_info, company_info=company_info)
        else: 
            raise ValueError(f"Ending on an account node: account info - {acc_info}")
    # Data node, extract data
    elif node_type == "Data":
        record = _extract_data_node(node=node,columns=columns,acc_info=acc_info, company_info=company_info)
        yield record
    else:
        raise ValueError(f"Unrecognized Node Type - node summary - {node['Summary']}")
    
    
def flatten_one_file(company:str, start:str, path:Path|str) -> Iterator[Dict[str,str]]:
    """
    Purpose:
        - flatten one file
        - call `_crawler()` on every node existing at the first level `obj["Rows"]["Row"]`
    Input:
        - `company`: company code
        - `start`: period start for the report, tied to naming of the bronze files, ISO format
        - `path`: root path to the bronze storage
    """
    file_path = resolve_json_path(company_code=company, start=start, raw_path=path)
    if file_path.exists():
        with open(file_path, "rb") as f:
            raw = f.read()
        obj = orjson.loads(raw)
        if obj.get("Rows", {}) and obj["Rows"].get("Row", []):
            cols = extract_column_meta(obj=obj)
            for node in obj["Rows"]["Row"]:
                yield from _crawler(node=node,columns=cols,company_info=company)

