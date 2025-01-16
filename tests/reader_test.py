import os
from tempfile import TemporaryDirectory

import lazy_pandas as lp
import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

ASSETS_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "assets"))


@pytest.fixture
def iceberg_table_uri():
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


def test_read_csv():
    wheather_statition_uri = os.path.join(ASSETS_PATH, "weather_station.csv")
    df = lp.read_csv(wheather_statition_uri, sep=";")
    assert df.columns == ["city", "temperature"]


def test_read_parquet():
    wheather_statition_uri = os.path.join(ASSETS_PATH, "weather_station.parquet")
    df = lp.read_parquet(wheather_statition_uri, columns=["temperature", "city"])
    assert df.columns == ["temperature", "city"]


def test_read_delta():
    delta_table_uri = os.path.join(ASSETS_PATH, "delta_table")
    df = lp.read_delta(delta_table_uri)
    assert df.columns == ["a", "b", "c"]


def test_read_iceberg(iceberg_table_uri):
    df = lp.read_iceberg(iceberg_table_uri)
    assert df.columns == ["lat", "long"]
    df = df.collect()
    assert df.shape == (3, 2)
    assert df.columns.tolist() == ["lat", "long"]
    assert df["lat"].tolist() == [52.371807, 52.387386, 52.078663]
    assert df["long"].tolist() == [4.896029, 4.646219, 4.288788]
