import duckdb
import numpy as np
import pandas as pd
import pytest

from lazy_pandas import LazyFrame
from lazy_pandas.exceptions import ContributionAcceptedError


# Create a decorator that will skip tests when ContributionAcceptedError is raised
def skip_on_contribution_error(func):
    """
    Decorator that automatically skips a test if ContributionAcceptedError is raised.
    """

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ContributionAcceptedError as e:
            pytest.skip(f"Skipped due to ContributionAcceptedError: {str(e)}")

    return wrapper


class DataFramePair:
    """
    Utility class for testing LazyFrame vs pandas DataFrame

    Facilitates running the same tests on LazyFrame and pandas DataFrame,
    allowing comparison of results to ensure equivalence.
    """

    def __init__(self, query=None, pandas_df=None, lazy_df=None):
        """
        Initializes a pair of DataFrames (LazyFrame and pandas) for testing

        Args:
            query: SQL query to create the dataframes
            pandas_df: Existing pandas DataFrame to use as base
            lazy_df: Existing LazyFrame to use as base
        """
        if query:
            self.rel = duckdb.sql(query)
            self.lazy_df = LazyFrame(self.rel)
            self.pandas_df = self.lazy_df.collect()
        elif pandas_df is not None:
            self.pandas_df = pandas_df
            # Convert pandas to DuckDB and then to LazyFrame
            self.lazy_df = LazyFrame(duckdb.from_df(pandas_df))
        elif lazy_df is not None:
            self.lazy_df = lazy_df
            self.pandas_df = lazy_df.collect()
        else:
            raise ValueError("Must provide query, pandas_df or lazy_df")

    def copy(self):
        """Returns a copy of the DataFrame pair"""
        result = DataFramePair.__new__(DataFramePair)
        result.lazy_df = self.lazy_df.copy() if hasattr(self, "lazy_df") else None
        result.pandas_df = self.pandas_df.copy() if hasattr(self, "pandas_df") else None
        return result

    def assert_equal(self, pandas_result, lazy_result):
        """
        Verifies if the results from pandas and lazy_pandas are equivalent.
        """
        if isinstance(pandas_result, pd.DataFrame) and isinstance(lazy_result, pd.DataFrame):
            pd.testing.assert_frame_equal(pandas_result, lazy_result, check_dtype=False)
        elif isinstance(pandas_result, pd.Series) and isinstance(lazy_result, pd.Series):
            pd.testing.assert_series_equal(pandas_result, lazy_result, check_dtype=False)
        elif isinstance(pandas_result, list) and isinstance(lazy_result, list):
            assert pandas_result == lazy_result
        elif isinstance(pandas_result, tuple) and isinstance(lazy_result, tuple):
            assert pandas_result == lazy_result
        elif np.isscalar(pandas_result) and np.isscalar(lazy_result):
            if pd.isna(pandas_result) and pd.isna(lazy_result):
                assert True
            else:
                assert pandas_result == lazy_result
        else:
            assert pandas_result == lazy_result


@pytest.fixture
def int_column_df():
    """Fixture that returns a DataFrame with integer columns for testing"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4")


@pytest.fixture
def multi_row_df():
    """Fixture that returns a DataFrame with multiple rows for testing"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4 UNION ALL SELECT 5, 6")


@pytest.fixture
def datetime_df():
    """Fixture that returns a DataFrame with datetime columns for testing"""
    return DataFramePair(
        query="""
        SELECT cast('2023-05-01' as datetime) AS dt_time
        UNION ALL
        SELECT cast('2024-01-02 15:00:00' as datetime)
    """
    )


@pytest.fixture
def simple_df_pair():
    """Fixture that creates a simple DataFrame/LazyFrame pair"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b")


@pytest.fixture
def multi_row_df_pair():
    """Fixture that creates a DataFrame/LazyFrame pair with multiple rows"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4")


@pytest.fixture
def numeric_df_pair():
    """Fixture that creates a DataFrame/LazyFrame pair with numeric data"""
    return DataFramePair(
        query="""
        SELECT 1 AS a, 2 AS b, 3 AS c
        UNION ALL SELECT 4, 5, 6
        UNION ALL SELECT 7, 8, 9
    """
    )


@pytest.fixture
def duplicate_df_pair():
    """Fixture that creates a DataFrame/LazyFrame pair with duplicate rows"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b UNION ALL SELECT 1, 2")


@pytest.fixture
def partial_duplicate_df_pair():
    """Fixture that creates a DataFrame/LazyFrame pair with partial duplication"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b UNION ALL SELECT 2, 2")
