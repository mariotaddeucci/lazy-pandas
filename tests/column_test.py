import duckdb
import pytest
from pandas_lazy import LazyFrame


@pytest.fixture
def df():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4")
    return LazyFrame(rel)


def test_negative_value(df):
    df["a"] = -df["a"]
    df = df.collect()
    assert df["a"].tolist() == [-1, -3]


def test_addition(df):
    df["c"] = df["a"] + df["b"]
    df = df.collect()
    assert df["c"].tolist() == [3, 7]


def test_subtraction(df):
    df["c"] = df["a"] - df["b"]
    df = df.collect()
    assert df["c"].tolist() == [-1, -1]


def test_multiplication(df):
    df["c"] = df["a"] * df["b"]
    df = df.collect()
    assert df["c"].tolist() == [2, 12]


def test_division(df):
    df["c"] = df["a"] / df["b"]
    df = df.collect()
    assert df["c"].tolist() == [0.5, 0.75]


def test_addition_constant(df):
    df["c"] = df["a"] + 2
    df = df.collect()
    assert df["c"].tolist() == [3, 5]


def test_cast_column(df):
    df["a"] = df["a"].astype(str)
    assert df.collect()["a"].tolist() == ["1", "3"]
    df["a"] = df["a"].astype(int)
    assert df.collect()["a"].tolist() == [1, 3]
    df["a"] = df["a"].astype(float)
    assert df.collect()["a"].tolist() == [1.0, 3.0]
