import duckdb
import pandas as pd
import pytest
from pandas_lazy import LazyFrame


@pytest.fixture
def df():
    rel = duckdb.sql("""
        SELECT cast('2023-05-01' as datetime) AS dt_time
        UNION ALL
        SELECT cast('2024-01-02 15:00:00' as datetime)
    """)
    return LazyFrame(rel)


def test_dt_date(df):
    df["dt_time"] = df["dt_time"].dt.date()
    df = df.collect()
    assert df["dt_time"].tolist() == [pd.Timestamp(2023, 5, 1), pd.Timestamp(2024, 1, 2)]


def test_dt_year(df):
    df["year"] = df["dt_time"].dt.year()
    df = df.collect()
    assert df["year"].tolist() == [2023, 2024]


def test_dt_quarter(df):
    df["quarter"] = df["dt_time"].dt.quarter()
    df = df.collect()
    assert df["quarter"].tolist() == [2, 1]


def test_dt_month(df):
    df["month"] = df["dt_time"].dt.month()
    df = df.collect()
    assert df["month"].tolist() == [5, 1]


def test_dt_day(df):
    df["day"] = df["dt_time"].dt.day()
    df = df.collect()
    assert df["day"].tolist() == [1, 2]
