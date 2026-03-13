"""
src.qbo_etl.silver.nested_reports

Purpose:
    - ingest raw Bronze nested JSON data structure, convert into tabular format and write out as spark data frame in parquet format

Exposed API:
    - `` - 
"""
from typing import Iterator, Dict

def _extract_column_meta(obj:dict) -> list[str]:
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
    cols = [item["ColTitle"] for item in meta]
    return cols

def _identify_node_type(node: dict) -> str:
    """
    Purpose:
        - input a node represented by 
            - category or account node or parent account data node: a dictionary with 3-4 keys `['Header' (optional), 'Rows', 'Summary', 'type']`
            - data node: a dictionary with 2 keys `['ColData', 'type']`
            - summary node: a dictionary with 2 keys `['Summary', 'type']`
        - output the exact node type
            - `Category`, `Category End`, `Account`, `Data`, `Summary Only`, `Include Data For Parent`
    """
    node_type = node.get("type", "")
    if node_type == "": raise ValueError(f"node['type'] is missing, and node type cannot be determined - node summary - {node['Summary']}")
    # Data Node
    if node_type == "Data": 
        return "Data"
    # summary node
    if "Header" not in node.keys() and "Rows" not in node.keys():
        return "Summary Only"
    # only three keys in the dictionary
    if "Header" not in node.keys():
        if node.get("Rows", {}) == {}: return "Category End" # Category End Node without nested child records
        else: return "Include Data For Parent" # subsequent transactions without header should inherit the account information here
    # Account Node
    header = node.get("Header", {})
    if header == {}: raise ValueError(f"Trying to access Header for determining account node, node keys - {node.keys()} missing header - node summary - {node['Summary']}")
    header_data = node["Header"].get("ColData", [])
    if header_data == []: raise ValueError(f"Trying to access Header for determining account node, node keys - {node.keys()} missing header info - node summary - {node['Summary']}")
    acc_id = header_data[0].get("id", "")
    # if missing id, it is a category node with nested sub-records
    if acc_id == "":
        return "Category"
    # if has id, it is an account node
    else:
        return "Account"

def _extract_data_node(node: dict, columns: list[str], acc_info:dict[str,str]) -> dict:
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
    return records

def _crawler(node:dict, columns: list[str], acc_info:dict[str,str]|None = None) -> Iterator[Dict[str,str]]:
    """
    Purpose:
        - generator that `yield` leaf node data records
    """
    node_type = _identify_node_type(node=node)
    # for Category End or Summary Only node, end here
    if node_type in ["Category End","Summary Only"]:
        return
    # for Category node, just continue with sub nodes or end if no sub nodes
    elif node_type in ["Category", "Include Data For Parent"]:
        if "Rows" in node and "Row" in node["Rows"]:
            for sub_node in node["Rows"]["Row"]:
                yield from _crawler(node=sub_node, columns=columns, acc_info=acc_info)
        else: 
            return
    # for Account node, extract info and continue
    elif node_type == "Account":
        acc_data = node["Header"]["ColData"][0]
        acc_info = {"AccFull": acc_data["value"], "AccID": acc_data["id"]}
        if "Rows" in node and "Row" in node["Rows"]:
            for sub_node in node["Rows"]["Row"]:
                yield from _crawler(node=sub_node, columns=columns, acc_info=acc_info)
        else: 
            raise ValueError(f"Ending on an account node: account info - {acc_info}")
    # Data node, extract data
    elif node_type == "Data":
        record = _extract_data_node(node=node,columns=columns,acc_info=acc_info)
        yield record
    else:
        raise ValueError(f"Unrecognized Node Type - node summary - {node['Summary']}")
    
    
