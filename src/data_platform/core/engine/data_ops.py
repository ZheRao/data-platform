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

def _create_fiscal_year_pd(df: pd.DataFrame, date_col:str, cut_off: int) -> pd.DataFrame:
    """
    Purpose:
        - create `fiscal_year` column with Pandas
    """
    # parse date
    df["date_parsed"] = pd.to_datetime(df[date_col], errors="coerce")
    valid_date_mask = df["date_parsed"].notna()
    df["is_valid_date"] = valid_date_mask
    # create default fiscal year
    df["fiscal_year"] = pd.NA
    df.loc[valid_date_mask, "fiscal_year"] = df.loc[valid_date_mask, "date_parsed"].dt.year
    # adjust for cut_off
    mask_cut_off = ((valid_date_mask) & (df["date_parsed"].dt.month>=cut_off))
    df.loc[mask_cut_off, "fiscal_year"] = df.loc[mask_cut_off, "fiscal_year"] + 1
    return df

def _create_fiscal_year_spark(df: SparkDF, date_col: str, cut_off: int) -> SparkDF:
    """
    Purpose:
        - create `fiscal_year` column with Spark
    """
    # parse date
    df = df.withColumn("date_parsed", F.try_to_date(F.col(date_col)))
    df = df.withColumn("is_valid_date", F.col("date_parsed").isNotNull())
    # build expression where when month >= cut_off -> year + 1, else year
    expr = (
        F.when(
            F.col("is_valid_date"),
            F.year(F.col("date_parsed")) + 
                F.when(F.month(F.col("date_parsed")) >= cut_off, 1)
                .otherwise(0)
        )
        .otherwise(F.lit(None))
    )
    # attach result with withColumn
    df = df.withColumn("fiscal_year", expr)
    return df
    



def create_fiscal_year(df, date_col:str, cut_off: int = 11):
    """
    Purpose:
        - create `parsed_date` `is_valid_date` column
        - create `fiscal_year` from `parsed_date` 
    Input:
        - `df`: dataframe with a date column
        - `date_col`: name of the date folumn
        - `cut_off`: the cut off month of the fiscal year start, numeric, e.g., 11 means fiscal year starts in November
    """
    if date_col not in df.columns: raise KeyError(f"{date_col} is not a column in df passed to create_fiscal_year function")
    if isinstance(df, pd.DataFrame):
        return _create_fiscal_year_pd(df=df, date_col=date_col,cut_off=cut_off)
    elif isinstance(df, SparkDF):
        return _create_fiscal_year_spark(df=df, date_col=date_col, cut_off=cut_off)
    else:
        raise TypeError(f"Expected Pandas or Spark DataFrame for `create_fiscal_year` function, received {type(df)}")

