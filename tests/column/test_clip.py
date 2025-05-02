import pandas as pd
import pytest

from conftest import DataFramePair


@pytest.fixture
def numeric_column_df():
    """Fixture that creates a DataFrame with numeric columns for operation tests"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4")


def test_clip_both_bounds(numeric_column_df):
    """Tests the clip method with both bounds (lower and upper)"""
    # Applying clip in LazyFrame
    numeric_column_df.lazy_df["c"] = numeric_column_df.lazy_df["a"].clip(2, 3)
    lazy_result = numeric_column_df.lazy_df.collect()

    # Applying clip in pandas for comparison
    pandas_df = numeric_column_df.pandas_df.copy()
    pandas_df["c"] = pandas_df["a"].clip(2, 3)

    # Verifications
    assert lazy_result["c"].tolist() == [2, 3]

    # Comparison with pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_clip_lower_bound(numeric_column_df):
    """Tests the clip method with only lower limit"""
    # Applying clip in LazyFrame
    numeric_column_df.lazy_df["c"] = numeric_column_df.lazy_df["a"].clip(lower=2)
    lazy_result = numeric_column_df.lazy_df.collect()

    # Applying clip in pandas for comparison
    pandas_df = numeric_column_df.pandas_df.copy()
    pandas_df["c"] = pandas_df["a"].clip(lower=2)

    # Verifications
    assert lazy_result["c"].tolist() == [2, 3]

    # Comparison with pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_clip_upper_bound(numeric_column_df):
    """Tests the clip method with only upper limit"""
    # Applying clip in LazyFrame
    numeric_column_df.lazy_df["c"] = numeric_column_df.lazy_df["a"].clip(upper=2)
    lazy_result = numeric_column_df.lazy_df.collect()

    # Applying clip in pandas for comparison
    pandas_df = numeric_column_df.pandas_df.copy()
    pandas_df["c"] = pandas_df["a"].clip(upper=2)

    # Verifications
    assert lazy_result["c"].tolist() == [1, 2]

    # Comparison with pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_clip_no_bounds(numeric_column_df):
    """Tests the clip method without limits (should return the same values)"""
    # Applying clip in LazyFrame
    numeric_column_df.lazy_df["c"] = numeric_column_df.lazy_df["a"].clip()
    lazy_result = numeric_column_df.lazy_df.collect()

    # Applying clip in pandas for comparison
    pandas_df = numeric_column_df.pandas_df.copy()
    pandas_df["c"] = pandas_df["a"].clip()

    # Verifications
    assert lazy_result["c"].tolist() == [1, 3]

    # Comparison with pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)
