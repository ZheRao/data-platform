"""
src.data_platform.sources.qbo.utils.contracts

Purpose:
    - standardized data structure for program-created inputs

Exposed Structures:
    - `TaskRecord` - data structure for tasks created for Spark jobs

"""

from typing import TypedDict

class TaskRecord(TypedDict):
    company: str
    start: str
    end: str