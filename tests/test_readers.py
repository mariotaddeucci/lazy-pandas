"""
Tests for reader functions (CSV, JSON, Parquet, Delta, Iceberg).
"""

import os
import tempfile
from io import StringIO
from tempfile import TemporaryDirectory

import lazy_pandas as lp
import pandas as pd
import pyarrow as pa
import pytest

from lazy_pandas import LazyFrame

ASSETS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "assets"))


@pytest.fixture
def iceberg_table_uri():
    """Fixture to create a temporary Iceberg table for testing."""
    pytest.importorskip("pyiceberg")
    from pyiceberg.catalog.sql import SqlCatalog

    with TemporaryDirectory() as temp_dir:
        catalog = SqlCatalog(
            "default",
            **{
                "uri": f"sqlite:///{temp_dir}/catalog.db",
                "warehouse": temp_dir,
            },
        )

        catalog.create_namespace_if_not_exists("default")
        df = pa.Table.from_pylist(
            [
                {"lat": 52.371807, "long": 4.896029},
                {"lat": 52.387386, "long": 4.646219},
                {"lat": 52.078663, "long": 4.288788},
            ],
        )
        table = catalog.create_table_if_not_exists("default.coordinates", schema=df.schema)
        table.overwrite(df)
        yield table.metadata_location.removeprefix("file://")


# CSV Tests


def test_read_csv_from_file():
    """Tests reading CSV from file using assets."""
    csv_path = os.path.join(ASSETS_PATH, "weather_station.csv")
    df = lp.read_csv(csv_path, sep=";")
    assert df.columns == ["city", "temperature"]


def test_read_csv_from_buffer():
    """Tests reading CSV from a buffer (StringIO)."""
    csv_data = "col1,col2\n1,a\n2,b\n3,c"
    buffer = StringIO(csv_data)

    lazy_df = lp.read_csv(buffer, header=True, sep=",")

    assert isinstance(lazy_df, LazyFrame)
    result = lazy_df.collect()
    assert result.shape == (3, 2)
    assert list(result.columns) == ["col1", "col2"]
    assert result["col1"].tolist() == [1, 2, 3]
    assert result["col2"].tolist() == ["a", "b", "c"]


def test_read_csv_with_options():
    """Tests reading CSV with various options."""
    with tempfile.NamedTemporaryFile(suffix=".csv", mode="w+", delete=False) as f:
        f.write("col1;col2;col3\n1;a;2020-01-01\n2;b;2020-01-02\n3;c;2020-01-03")
        temp_path = f.name

    try:
        lazy_df = lp.read_csv(
            temp_path, sep=";", header=True, parse_dates=["col3"], all_varchar=True, normalize_names=True
        )

        assert isinstance(lazy_df, LazyFrame)
        result = lazy_df.collect()
        assert result.shape == (3, 3)
        assert list(result.columns) == ["col1", "col2", "col3"]
        assert pd.api.types.is_datetime64_dtype(result["col3"].dtype)
    finally:
        os.unlink(temp_path)


# Parquet Tests


def test_read_parquet_from_file():
    """Tests reading Parquet from file using assets."""
    parquet_path = os.path.join(ASSETS_PATH, "weather_station.parquet")
    df = lp.read_parquet(parquet_path, columns=["temperature", "city"])
    assert df.columns == ["temperature", "city"]


def test_read_parquet_with_options():
    """Tests reading Parquet files with various options."""
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [4.1, 5.2, 6.3]})

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        temp_path = f.name

    try:
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
        os.unlink(temp_path)


# Delta Tests


def test_read_delta():
    """Tests reading Delta Lake tables."""
    delta_table_uri = os.path.join(ASSETS_PATH, "delta_table")
    if not os.path.exists(delta_table_uri):
        pytest.skip("Test delta_table file not found")

    df = lp.read_delta(delta_table_uri)
    assert df.columns == ["a", "b", "c"]

    result = df.collect()
    assert isinstance(result, pd.DataFrame)


# Iceberg Tests


def test_read_iceberg(iceberg_table_uri):
    """Tests reading Apache Iceberg tables."""
    df = lp.read_iceberg(iceberg_table_uri)
    assert df.columns == ["lat", "long"]
    df = df.collect()
    assert df.shape == (3, 2)
    assert df.columns.tolist() == ["lat", "long"]
    assert df["lat"].tolist() == [52.371807, 52.387386, 52.078663]
    assert df["long"].tolist() == [4.896029, 4.646219, 4.288788]
