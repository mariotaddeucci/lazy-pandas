import os
import tempfile
from io import StringIO

import pandas as pd
import pytest

import lazy_pandas as lp
from lazy_pandas import LazyFrame


def test_from_pandas():
    """Tests conversion from a pandas DataFrame to LazyFrame."""
    # Create a pandas DataFrame
    pandas_df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    # Convert to LazyFrame
    lazy_df = lp.from_pandas(pandas_df)

    # Verifications
    assert isinstance(lazy_df, LazyFrame)
    result = lazy_df.collect()
    pd.testing.assert_frame_equal(result, pandas_df)


def test_read_csv_from_buffer():
    """Tests reading CSV from a buffer (StringIO)."""
    # Create a buffer with CSV data
    csv_data = "col1,col2\n1,a\n2,b\n3,c"
    buffer = StringIO(csv_data)

    # Read from buffer with different options
    lazy_df = lp.read_csv(buffer, header=True, sep=",")

    # Verifications
    assert isinstance(lazy_df, LazyFrame)
    result = lazy_df.collect()
    assert result.shape == (3, 2)
    assert list(result.columns) == ["col1", "col2"]

    # Values are automatically converted to integers, so we adjust the test
    assert result["col1"].tolist() == [1, 2, 3]
    assert result["col2"].tolist() == ["a", "b", "c"]


def test_read_csv_with_options():
    """Tests reading CSV with various options."""
    # Create a temporary file with CSV data
    with tempfile.NamedTemporaryFile(suffix=".csv", mode="w+", delete=False) as f:
        f.write("col1;col2;col3\n1;a;2020-01-01\n2;b;2020-01-02\n3;c;2020-01-03")
        temp_path = f.name

    try:
        # Test with different options
        lazy_df = lp.read_csv(
            temp_path, sep=";", header=True, parse_dates=["col3"], all_varchar=True, normalize_names=True
        )

        # Verifications
        assert isinstance(lazy_df, LazyFrame)
        result = lazy_df.collect()
        assert result.shape == (3, 3)
        assert list(result.columns) == ["col1", "col2", "col3"]

        # Verify if col3 was correctly parsed as a date
        assert pd.api.types.is_datetime64_dtype(result["col3"].dtype)
    finally:
        # Clean up temporary file
        os.unlink(temp_path)


def test_read_parquet():
    """Tests reading Parquet files."""
    # We need a parquet file for testing
    # Let's create a dataframe and save it as parquet in a temporary file
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [4.1, 5.2, 6.3]})

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        temp_path = f.name

    try:
        # Save as parquet
        df.to_parquet(temp_path)

        # Test complete reading
        lazy_df = lp.read_parquet(temp_path)
        assert isinstance(lazy_df, LazyFrame)
        result = lazy_df.collect()
        pd.testing.assert_frame_equal(result, df)

        # Test reading specific columns
        lazy_df_cols = lp.read_parquet(temp_path, columns=["a", "c"])
        result_cols = lazy_df_cols.collect()
        assert list(result_cols.columns) == ["a", "c"]
        pd.testing.assert_frame_equal(result_cols, df[["a", "c"]])

    finally:
        # Clean up temporary file
        os.unlink(temp_path)


@pytest.mark.skipif(
    not os.path.exists(os.path.join(os.path.dirname(__file__), "assets/delta_table")),
    reason="Test delta_table file not found",
)
def test_read_delta():
    """Tests reading Delta Lake tables."""
    # Uses the example delta_table file in /tests/assets/ if it exists
    delta_dir = os.path.join(os.path.dirname(__file__), "assets/delta_table")

    if os.path.exists(delta_dir):
        # Read the Delta table
        lazy_df = lp.read_delta(delta_dir)

        # Verifications
        assert isinstance(lazy_df, LazyFrame)
        result = lazy_df.collect()
        assert isinstance(result, pd.DataFrame)
        # We verify that we can collect the result without errors
        # The specific columns depend on the content of the test Delta table


@pytest.mark.skipif(True, reason="Depends on specific Iceberg configuration")
def test_read_iceberg():
    """
    Tests reading Apache Iceberg tables.
    This test is disabled by default because it depends on a valid Iceberg table.
    """
    # This test will only work if we have a valid Iceberg table to test
    # Uses the iceberg_table_uri fixture from reader_test.py if available
    from tests.reader_test import iceberg_table_uri

    table_path = iceberg_table_uri()

    # Read the Iceberg table
    lazy_df = lp.read_iceberg(table_path)

    # Verifications
    assert isinstance(lazy_df, LazyFrame)
    result = lazy_df.collect()
    assert isinstance(result, pd.DataFrame)
    assert "lat" in result.columns
    assert "long" in result.columns
