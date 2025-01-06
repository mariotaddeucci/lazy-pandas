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
    df["dt_time"] = df["dt_time"].dt.date
    df = df.collect()
    assert df["dt_time"].tolist() == [pd.Timestamp(2023, 5, 1), pd.Timestamp(2024, 1, 2)]


def test_dt_year(df):
    df["year"] = df["dt_time"].dt.year
    df = df.collect()
    assert df["year"].tolist() == [2023, 2024]


def test_dt_quarter(df):
    df["quarter"] = df["dt_time"].dt.quarter
    df = df.collect()
    assert df["quarter"].tolist() == [2, 1]


def test_dt_month(df):
    df["month"] = df["dt_time"].dt.month
    df = df.collect()
    assert df["month"].tolist() == [5, 1]


def test_dt_day(df):
    df["day"] = df["dt_time"].dt.day
    df = df.collect()
    assert df["day"].tolist() == [1, 2]


def test_dt_is_month_start(df):
    df["is_month_start"] = df["dt_time"].dt.is_month_start
    df = df.collect()
    assert df["is_month_start"].tolist() == [True, False]


def test_dt_is_quarter_start():
    df = LazyFrame(
        duckdb.sql("select cast('2023-01-02' as datetime) as dt_time union select cast('2023-04-01' as datetime)")
    )
    df["is_quarter_start"] = df["dt_time"].dt.is_quarter_start
    df = df.sort_values("dt_time")
    df = df.collect()
    assert df["is_quarter_start"].tolist() == [False, True]


def test_dt_is_year_start():
    df = LazyFrame(
        duckdb.sql("select cast('2023-01-02' as datetime) as dt_time union select cast('2023-01-01' as datetime)")
    )
    df = df.sort_values("dt_time")
    df["is_year_start"] = df["dt_time"].dt.is_year_start
    df = df.collect()
    assert df["is_year_start"].tolist() == [True, False]


def test_dt_is_month_end():
    df = LazyFrame(
        duckdb.sql("select cast('2023-05-01' as datetime) as dt_time union select cast('2023-05-31' as datetime)")
    )
    df["is_month_end"] = df["dt_time"].dt.is_month_end
    df = df.sort_values("dt_time")
    df = df.collect()
    assert df["is_month_end"].tolist() == [False, True]


def test_dt_is_year_end():
    df = LazyFrame(
        duckdb.sql("select cast('2023-12-31' as datetime) as dt_time union select cast('2023-01-01' as datetime)")
    )
    df = df.sort_values("dt_time")
    df["is_year_end"] = df["dt_time"].dt.is_year_end
    df = df.collect()
    assert df["is_year_end"].tolist() == [False, True]


def test_dt_weekday(df):
    df["weekday"] = df["dt_time"].dt.weekday()
    df = df.collect()
    assert df["weekday"].tolist() == [1, 2]
