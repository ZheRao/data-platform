"""
src.data_platform.sources.qbo.transformation.pl

Purpose:
    - transform ingested raw PL report from QBO

Exposed API:
    - `transform_pl_spark()` 
    - `transform_pl_pandas()`
    
Notes:
    - assumptions
        - `date` is the column for storing dates from QBO API
        - currently `cut_off` external config location is undecided
    - caution
        - for example, November cut_off -> reading preivous year's quarter file -> reading October of previous years' records -> got unwanted fiscal_year's record hanging
        - fix: filter through scope fiscal_year only before repartition and write
        - caution: look at designs for `create_task` and raw QBO pulls to ensure other `cut_off` is accommodated
"""
from __future__ import annotations 
from pyspark.sql import SparkSession, DataFrame as SparkDF, functions as F
import pandas as pd
from pathlib import Path

from data_platform.core.engine.spark import generate_default_schema
from data_platform.core.engine.data_ops import create_fiscal_year
from data_platform.core.utils.filesystem import read_configs

from data_platform.sources.qbo.transformation.single_file_traversal import flatten_one_file
from data_platform.sources.qbo.transformation.schema_discovery import compose_column_superset
from data_platform.sources.qbo.utils.contracts import TaskRecord

def transform_pl_spark(tasks: list[TaskRecord], scope:range|list[int], spark:SparkSession, path_config:dict) -> SparkDF:
    """
    Purpose:
        - read raw JSON files, prepare tabulated PL report with Spark
    Inputs:
        - `tasks`: list of dictionaries contain `['company', 'start', 'end']`, each one corresponds to a Spark job (reading file, flatten)
        - `scope`: `range` or list of integers representing fiscal_years for the flatten job
        - `spark`: `SparkSession` object for performing actual spark jobs
        - `path_config`: the external path config JSON file, dictionary format
    """

    # figure out the bronze path
    raw_path = Path(path_config["root"]) / path_config["bronze"]["pl"]

    # get spark configuration
    spark_config = read_configs(source_system="qbo", config_type="system", name="spark.json")

    # create parallel tasks
    sc = spark.sparkContext
    rdd = sc.parallelize(tasks, numSlices = spark_config["run_time"]["num_cores"] * spark_config["run_time"]["slice_multiplier"])

    # create raw_path broadcast
    raw_path_bc = sc.broadcast(raw_path)

    # define the wrapper for per partition spark code
    def _flatten_file(records):
        path = raw_path_bc.value
        for record in records:
            yield from flatten_one_file(company=record["company"],start=record["start"],path=path)
    
    # run the job
    rdd2 = rdd.mapPartitions(_flatten_file)

    # discover all columns and create default schema
    final_columns = compose_column_superset(tasks=tasks, raw_path=raw_path)
    default_schema = generate_default_schema(columns = final_columns)
    print("\nDiscovered columns superset and composed default schema")

    # create df and de-dup
    df = spark.createDataFrame(rdd2, schema=default_schema)
    df = df.dropDuplicates()

    # print(f"\n {df.count()} rows of data processed")

    # create fiscal_year
    df2 = create_fiscal_year(df=df, date_col="date", cut_off=11)        # external config

    # save
    pl_path = Path(path_config["root"]) / path_config["silver"]["pl"]
    (
        df2
            .filter(F.col("fiscal_year").isin(list(scope)))
            .repartition(len(scope), "fiscal_year") 
            .write.format(spark_config["write"]["format"]) 
            .mode(spark_config["write"]["mode"]) 
            .partitionBy("fiscal_year") 
            .save(str(pl_path / "spark"))
    )

    return df2

def transform_pl_pandas(tasks: list[TaskRecord], scope:range|list[int], path_config:dict) -> pd.DataFrame:
    """
    Purpose:
        - read raw JSON files, prepare tabulated PL report with Pandas
    Inputs:
        - `tasks`: list of dictionaries contain `['company', 'start', 'end']`, each one corresponds to a Spark job (reading file, flatten)
        - `scope`: `range` or list of integers representing fiscal_years for the flatten job
        - `path_config`: the external path config JSON file, dictionary format
    Note:
        - one parquet per fiscal year
    Warning:
        - all data will be held in memory
    """
    # figure out the bronze path
    raw_path = Path(path_config["root"]) / path_config["bronze"]["pl"]

    # discover all columns
    final_columns = compose_column_superset(tasks=tasks, raw_path=raw_path)
    
    # holding all records
    records = []

    for task in tasks:
        records.extend(list(flatten_one_file(company=task["company"], start=task["start"], path=raw_path)))
    
    df = pd.DataFrame(records).reindex(columns=final_columns)
    df = df.drop_duplicates()

    df = create_fiscal_year(df=df, date_col="date", cut_off=11)

    df = df[df["fiscal_year"].isin(list(scope))]
    print(f"Total rows processed: {len(df)}")

    # saving
    pl_path = Path(path_config["root"]) / path_config["silver"]["pl"] / "pandas"
    for fy, chunk in df.groupby("fiscal_year", sort=False):
        save_path = pl_path / f"fiscal_year={fy}"
        save_path.mkdir(exist_ok=True, parents=True)
        chunk.to_parquet(path=save_path/"pl.parquet")
    return df
    


