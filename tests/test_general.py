"""
Tests for general utility functions (e.g., from_pandas).
"""

import pandas as pd

import lazy_pandas as lp
from lazy_pandas import LazyFrame


def test_from_pandas():
    """Tests conversion from a pandas DataFrame to LazyFrame."""
    pandas_df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    lazy_df = lp.from_pandas(pandas_df)

    assert isinstance(lazy_df, LazyFrame)
    result = lazy_df.collect()
    pd.testing.assert_frame_equal(result, pandas_df)
