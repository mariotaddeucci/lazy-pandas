import duckdb
import pytest
from pandas_lazy import LazyFrame


@pytest.fixture
def df():
    rel = duckdb.sql("SELECT ' CUSTOM_string ' AS col1")
    return LazyFrame(rel)


def test_str_lower(df):
    df["col1"] = df["col1"].str.lower()
    df = df.collect()
    assert df["col1"].tolist() == [" custom_string "]


def test_str_upper(df):
    df["col1"] = df["col1"].str.upper()
    df = df.collect()
    assert df["col1"].tolist() == [" CUSTOM_STRING "]


def test_str_strip(df):
    df["col1"] = df["col1"].str.strip()
    df = df.collect()
    assert df["col1"].tolist() == ["CUSTOM_string"]


def test_str_lstrip(df):
    df["col1"] = df["col1"].str.lstrip()
    df = df.collect()
    assert df["col1"].tolist() == ["CUSTOM_string "]


def test_str_rstrip(df):
    df["col1"] = df["col1"].str.rstrip()
    df = df.collect()
    assert df["col1"].tolist() == [" CUSTOM_string"]


def test_str_len(df):
    df["col1"] = df["col1"].str.len()
    df = df.collect()
    assert df["col1"].tolist() == [15]


def test_str_replace(df):
    df["col1"] = df["col1"].str.replace("C", "X")
    df = df.collect()
    assert df["col1"].tolist() == [" XUSTOM_string "]


def test_str_startswith(df):
    df["col1"] = df["col1"].str.startswith(" C")
    df = df.collect()
    assert df["col1"].tolist() == [True]


def test_str_endswith(df):
    df["col1"] = df["col1"].str.endswith("g ")
    df = df.collect()
    assert df["col1"].tolist() == [True]


def test_str_contains(df):
    df["col1"] = df["col1"].str.contains("string")
    df = df.collect()
    assert df["col1"].tolist() == [True]
