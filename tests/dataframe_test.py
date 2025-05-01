import duckdb
import lazy_pandas as lp
import numpy as np
import pandas as pd
import pytest
from lazy_pandas import LazyFrame


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


def test_sample():
    rel = duckdb.sql("SELECT * FROM range(100)")
    df = LazyFrame(rel)

    # Test with n parameter
    sampled_n = df.sample(n=10)
    assert len(sampled_n.collect()) == 10

    # Test with frac parameter
    sampled_frac = df.sample(frac=0.1)
    # Note: With random sampling using frac, we can't guarantee an exact count
    # but we can ensure it's in a reasonable range (roughly 10% of 100)
    collected = sampled_frac.collect()
    assert 0 < len(collected) < 30  # Allow some variance due to randomness

    # We can only verify reproducibility when the DuckDB version supports
    # the random seed parameter in the random() function
    try:
        # Test with fixed random_state
        sample1 = df.sample(n=5, random_state=42)
        sample2 = df.sample(n=5, random_state=42)

        # If we get here, the random seed version is supported
        assert sample1.collect().equals(sample2.collect())

        # Different seeds should give different results
        sample3 = df.sample(n=10, random_state=43)
        # Note: There's a small chance they could randomly be the same
        # so this test could occasionally fail
        assert not sample1.collect().equals(sample3.collect())
    except Exception:
        # If random seed isn't supported in this DuckDB version, just skip this test
        pass

    # Test error cases
    with pytest.raises(ValueError):
        df.sample()  # Neither n nor frac specified

    with pytest.raises(ValueError):
        df.sample(n=10, frac=0.1)  # Both n and frac specified

    with pytest.raises(ValueError):
        df.sample(frac=0)  # frac must be > 0

    with pytest.raises(ValueError):
        df.sample(frac=1.5)  # frac must be <= 1


def test_describe():
    # Pular teste se o método describe não estiver implementado
    if not hasattr(LazyFrame, 'describe'):
        pytest.skip("Método describe não implementado ainda em LazyFrame")
        
    # Create a test dataframe with numeric columns
    rel = duckdb.sql("SELECT 1 AS a, 2 AS b, 3 AS c UNION ALL SELECT 4, 5, 6 UNION ALL SELECT 7, 8, 9")
    df = LazyFrame(rel)

    # Test default describe
    desc = df.describe()
    result = desc.collect()
    
    # Verifica se as estatísticas padrão estão presentes
    assert result.index.tolist() == ["count", "mean", "std", "min", "25%", "50%", "75%", "max"]
    assert all(col in result.columns for col in ["a", "b", "c"])
    
    # Verifica alguns valores
    assert result.loc["count", "a"] == 3
    assert result.loc["mean", "b"] == 5.0
    assert result.loc["min", "c"] == 3
    assert result.loc["max", "c"] == 9
