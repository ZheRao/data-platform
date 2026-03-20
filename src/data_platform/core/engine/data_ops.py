"""
src.data_platform.core.engine.data_ops

Purpose:
    - expose cross-source data operations
    - if `spark==True`, spark methods would be used, if not, pandas would be used

Exposed API:
    - `create_fiscal_year()` - create `fiscal_year` column
"""


from pyspark.sql import DataFrame as SparkDF, functions as F
import pandas as pd
import datetime as dt

def _create_fiscal_year_pd(df: pd.DataFrame, date_col:str, cut_off: int) -> pd.DataFrame:
    """
    Purpose:
        - create `fiscal_year` column with Pandas
    """
    df[date_col] = pd.to_datetime(df[date_col])
    df["month"] = df[date_col].dt.month
    df["fiscal_year"] = df[date_col].dt.year
    mask = df["month"] >= cut_off
    df.loc[mask, "fiscal_year"] += 1
    df = df.drop(columns=["month"])
    return df

def _create_fiscal_year_spark(df: SparkDF, date_col: str, cut_off: int) -> SparkDF:
    """
    Purpose:
        - create `fiscal_year` column with Spark
    """
    # convert to date format
    df = df.withColumn(date_col, F.to_date(F.col(date_col)))
    # build expression where when month >= cut_off -> year + 1, else year
    expr = (
        F.year(F.col(date_col)) + 
        F.when(F.month(F.col(date_col))>=cut_off, 1)
         .otherwise(0)
    )
    # attach result with withColumn
    df = df.withColumn("fiscal_year", expr)
    return df
    



def create_fiscal_year(df, date_col:str, cut_off: int = 11):
    """
    Purpose:
        - create `fiscal_year` column
    Input:
        - `df`: dataframe with a date column
        - `date_col`: name of the date folumn
        - `spark`: whether the data frame is `pyspark.sql.DataFrame` or `pandas.DataFrame`
        - `cut_off`: the cut off month of the fiscal year start, numeric, e.g., 11 means fiscal year starts in November
    """
    if date_col not in df.columns: raise KeyError(f"{date_col} is not a column in df passed to create_fiscal_year function")
    if isinstance(df, pd.DataFrame):
        return _create_fiscal_year_pd(df=df, date_col=date_col,cut_off=cut_off)
    elif isinstance(df, SparkDF):
        return _create_fiscal_year_spark(df=df, date_col=date_col, cut_off=cut_off)
    else:
        raise TypeError(f"Expected Pandas or Spark DataFrame for `create_fiscal_year` function, received {type(df)}")

