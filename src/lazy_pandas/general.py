"""
General utility functions for lazy_pandas.

This module provides general conversion and utility functions.
For data reading functions, see lazy_pandas.readers module.
"""

import duckdb
from lazy_pandas.frame.lazy_frame import LazyFrame

# Re-export reader functions for backward compatibility
from lazy_pandas.readers import read_csv, read_delta, read_iceberg, read_json, read_parquet


def from_pandas(df) -> LazyFrame:
    """
    Converts a pandas DataFrame to a LazyFrame.

    Args:
        df (pd.DataFrame): The pandas DataFrame to convert.

    Returns:
        LazyFrame: A LazyFrame containing the data from the pandas DataFrame.

    Example:
    ```python
    import pandas as pd
    import lazy_pandas as lp
    df = pd.DataFrame({'column1': [1, 2, 3], 'column2': ['a', 'b', 'c']})
    lazy_df = lp.from_pandas(df)
    ```
    """
    return LazyFrame(duckdb.from_df(df))
