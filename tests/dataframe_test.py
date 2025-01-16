import duckdb
import lazy_pandas as lp
import numpy as np
import pandas as pd


def test_list_columns():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b")
    df = lp.LazyFrame(rel)
    assert df.columns == ["a", "b"]
    for col_name in df.columns:
        assert isinstance(col_name, str)


def test_collect():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b")
    df = lp.LazyFrame(rel)
    df = df.collect()
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ["a", "b"]


def test_new_column():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b")
    df = lp.LazyFrame(rel)
    df["c"] = 3
    df = df.collect()
    assert df.shape == (1, 3)
    assert df.columns.tolist() == ["a", "b", "c"]
    assert df["c"].tolist() == [3]


def test_overwrite_column():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b")
    df = lp.LazyFrame(rel)
    df["a"] = 3
    df = df.collect()
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ["a", "b"]
    assert df["a"].tolist() == [3]


def test_select_columns():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b")
    df = lp.LazyFrame(rel)
    df = df[["b"]]
    df = df.collect()
    assert df.shape == (1, 1)
    assert df.columns.tolist() == ["b"]


def test_head():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4")
    df = lp.LazyFrame(rel)
    df = df.head(1)
    df = df.collect()
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ["a", "b"]


def test_sort_values():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4")
    df = lp.LazyFrame(rel)
    df = df.sort_values("b")
    df = df.collect()
    assert df.shape == (2, 2)
    assert df.columns.tolist() == ["a", "b"]
    assert df["b"].tolist() == [2, 4]


def test_drop_duplicates():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b UNION ALL SELECT 1, 2")
    df = lp.LazyFrame(rel)
    df = df.drop_duplicates()
    df = df.collect()
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ["a", "b"]


def test_drop_duplicates_subset():
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b UNION ALL SELECT 2, 2")
    df = lp.LazyFrame(rel)
    df = df.drop_duplicates(subset=["b"])
    df = df.collect()
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ["a", "b"]


def test_merge_inner():
    rel1 = duckdb.sql("SELECT 1 AS a, 2 AS b")
    rel2 = duckdb.sql("SELECT 1 AS a, 4 AS d")
    df1 = lp.LazyFrame(rel1)
    df2 = lp.LazyFrame(rel2)

    df = df1.merge(df2, on="a")
    df = df.collect()
    assert df.shape == (1, 3)
    assert sorted(df.columns.tolist()) == ["a", "b", "d"]


def test_merge_outer():
    rel1 = duckdb.sql("SELECT 1 AS a, 2 AS b")
    rel2 = duckdb.sql("SELECT 2 AS a, 4 AS d")
    df1 = lp.LazyFrame(rel1)
    df2 = lp.LazyFrame(rel2)

    df = df1.merge(df2, on="a", how="outer")
    df.sort_values("a", inplace=True)

    df = df.collect()

    assert df.shape == (2, 3)
    assert sorted(df.columns.tolist()) == ["a", "b", "d"]
    vl1, vl2 = df["b"].tolist()
    assert vl1 == 2
    assert np.isnan(vl2)
    vl1, vl2 = df["d"].tolist()
    assert np.isnan(vl1)
    assert vl2 == 4
    vl1, vl2 = df["a"].tolist()
    assert vl1 == 1
    assert vl2 == 2


def test_merge_left():
    rel1 = duckdb.sql("SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 3")
    rel2 = duckdb.sql("SELECT 1 AS a, 4 AS d UNION ALL SELECT 2, 5")
    df1 = lp.LazyFrame(rel1)
    df2 = lp.LazyFrame(rel2)

    df = df1.merge(df2, on="a", how="left")
    df.sort_values("a", inplace=True)
    df = df.collect()

    assert df.shape == (2, 3)
    assert sorted(df.columns.tolist()) == ["a", "b", "d"]
    vl1, vl2 = df["b"].tolist()
    assert vl1 == 2
    assert vl2 == 3

    vl1, vl2 = df["d"].tolist()
    assert vl1 == 4
    assert np.isnan(vl2)

    vl1, vl2 = df["a"].tolist()
    assert vl1 == 1
    assert vl2 == 3


def test_merge_right():
    rel1 = duckdb.sql("SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 3")
    rel2 = duckdb.sql("SELECT 1 AS a, 4 AS d UNION ALL SELECT 2, 5")
    df1 = lp.LazyFrame(rel2)
    df2 = lp.LazyFrame(rel1)

    df = df1.merge(df2, on="a", how="right")
    df.sort_values("a", inplace=True)
    df = df.collect()

    assert df.shape == (2, 3)
    assert sorted(df.columns.tolist()) == ["a", "b", "d"]
    vl1, vl2 = df["b"].tolist()
    assert vl1 == 2
    assert vl2 == 3

    vl1, vl2 = df["d"].tolist()
    assert vl1 == 4
    assert np.isnan(vl2)

    vl1, vl2 = df["a"].tolist()
    assert vl1 == 1
    assert vl2 == 3
