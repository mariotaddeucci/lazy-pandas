import duckdb
import pandas as pd
import pandas_lazy as pdl


def test_list_columns():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b")
    df = pdl.LazyFrame(rel)
    assert df.columns == ["a", "b"]
    for col_name in df.columns:
        assert isinstance(col_name, str)


def test_collect():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b")
    df = pdl.LazyFrame(rel)
    df = df.collect()
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ["a", "b"]


def test_new_column():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b")
    df = pdl.LazyFrame(rel)
    df["c"] = 3
    df = df.collect()
    assert df.shape == (1, 3)
    assert df.columns.tolist() == ["a", "b", "c"]
    assert df["c"].tolist() == [3]


def test_overwrite_column():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b")
    df = pdl.LazyFrame(rel)
    df["a"] = 3
    df = df.collect()
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ["a", "b"]
    assert df["a"].tolist() == [3]


def test_select_columns():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b")
    df = pdl.LazyFrame(rel)
    df = df[["b"]]
    df = df.collect()
    assert df.shape == (1, 1)
    assert df.columns.tolist() == ["b"]


def test_head():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4")
    df = pdl.LazyFrame(rel)
    df = df.head(1)
    df = df.collect()
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ["a", "b"]


def test_sort_values():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4")
    df = pdl.LazyFrame(rel)
    df = df.sort_values("b")
    df = df.collect()
    assert df.shape == (2, 2)
    assert df.columns.tolist() == ["a", "b"]
    assert df["b"].tolist() == [2, 4]


def test_drop_duplicates():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b UNION ALL SELECT 1, 2")
    df = pdl.LazyFrame(rel)
    df = df.drop_duplicates()
    df = df.collect()
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ["a", "b"]


def test_drop_duplicates_subset():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b UNION ALL SELECT 2, 2")
    df = pdl.LazyFrame(rel)
    df = df.drop_duplicates(subset=["b"])
    df = df.collect()
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ["a", "b"]
