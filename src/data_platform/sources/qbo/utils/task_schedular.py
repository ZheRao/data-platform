"""
src.data_platform.sources.qbo.transformation.task_schedular

Purpose:
    - create spark jobs as a list of dictionaries, so the meta-data (invariants) of each job is encoded into one single element of the list

Exposed API:
    - `create_jobs` - returns a list of dictionaries with keys `['company', 'start', 'end']`

Note for creating jobs:
    - Must have 
        - company
        - start
        - end
    - Other consideration
        - auth token: instead of appending the token to each task and take up a lot of space,   
        just broadcast the auth dictionary, after refresh every company, to ingest Spark jobs
        - bronze path: can be derived from company + start_year + start_month
        - silver path: Spark partitioned by Company and Fiscal Year - broadcast to jobs
"""

from __future__ import annotations
import datetime as dt
from typing import TypedDict, Sequence, Optional

# task schedule schema contract
class FlattenTask(TypedDict):
    company: str
    start: str   # ISO date
    end: str     # ISO date
    out_path: str

## hypter parameters
_LAST_DAY = {3: 31, 6: 30, 9: 30, 12: 31} # mapping exact date of quarter end for exact ending dates
_QUARTER_START_MONTHS = (1, 4, 7, 10)

def create_jobs(
    companies: Sequence[str],
    scope: Optional[Sequence[int]] = None,
) -> list[FlattenTask]:
    """
    Create a list of tasks for partitioned flattening of PL/GL report, converting semi-structured JSON file into tabular format for Parquet storage, example:
        {
            "company": "xxx",
            "start": "2024-10-01",
            "end": "2024-12-31",
        }
    """
    # set default scope if not passed
    if not scope:
        scope = [2025, 2026]
    today = dt.date.today()
    tasks: list[FlattenTask] = []
    for company in companies:
        # because FY starts in November of last calender year, include the quarter that begins Oct 1 because the bronze filenames are quarter-based and start in October
        earlist_fy = min(scope)     
        start = dt.date(earlist_fy-1, 10, 1)    
        end = dt.date(earlist_fy-1, 12, 31)
        tasks.append({
            "company": company, 
            "start": start.isoformat(),
            "end": end.isoformat(),
        })
        # create task for every quarter in the scope - skip quarters that hasn't come 
        for year in scope:  
            for month in _QUARTER_START_MONTHS:
                if dt.date(year, month, 1) > today: # skip creating this task if the intended quarter start date is after today - avoid extracting empty data
                    continue 
                start = dt.date(year, month, 1)
                end = dt.date(year, month+2, _LAST_DAY[month+2])
                tasks.append({
                    "company": company, 
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                })
    return tasks