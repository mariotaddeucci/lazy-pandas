import duckdb
import numpy as np
import pandas as pd
import pytest

from conftest import DataFramePair
from lazy_pandas import LazyFrame


@pytest.fixture
def merge_df_pairs():
    """Fixture que cria dois pares de DataFrames para teste de merge"""
    rel1 = pd.DataFrame({"a": [1], "b": [2]})
    rel2 = pd.DataFrame({"a": [1], "d": [4]})
    
    df_pair1 = DataFramePair(pandas_df=rel1)
    df_pair2 = DataFramePair(pandas_df=rel2)
    
    return df_pair1, df_pair2


@pytest.fixture
def outer_merge_df_pairs():
    """Fixture que cria dois pares de DataFrames para teste de merge outer"""
    rel1 = pd.DataFrame({"a": [1], "b": [2]})
    rel2 = pd.DataFrame({"a": [2], "d": [4]})
    
    df_pair1 = DataFramePair(pandas_df=rel1)
    df_pair2 = DataFramePair(pandas_df=rel2)
    
    return df_pair1, df_pair2


@pytest.fixture
def left_merge_df_pairs():
    """Fixture que cria dois pares de DataFrames para teste de merge left"""
    rel1 = pd.DataFrame({"a": [1, 3], "b": [2, 3]})
    rel2 = pd.DataFrame({"a": [1, 2], "d": [4, 5]})
    
    df_pair1 = DataFramePair(pandas_df=rel1)
    df_pair2 = DataFramePair(pandas_df=rel2)
    
    return df_pair1, df_pair2


def test_merge_inner(merge_df_pairs):
    """Testa o merge inner entre dois LazyFrames comparando com pandas"""
    df_pair1, df_pair2 = merge_df_pairs
    
    # Merge com LazyFrame
    lazy_result = df_pair1.lazy_df.merge(df_pair2.lazy_df, on="a").collect()
    
    # Merge com pandas
    pandas_result = df_pair1.pandas_df.merge(df_pair2.pandas_df, on="a")
    
    # Verificações
    assert lazy_result.shape == (1, 3)
    assert sorted(lazy_result.columns.tolist()) == ["a", "b", "d"]
    
    # Comparação com pandas (ordenando colunas para garantir consistência)
    pd.testing.assert_frame_equal(
        lazy_result[sorted(lazy_result.columns)], 
        pandas_result[sorted(pandas_result.columns)],
        check_dtype=False
    )


def test_merge_outer(outer_merge_df_pairs):
    """Testa o merge outer entre dois LazyFrames comparando com pandas"""
    df_pair1, df_pair2 = outer_merge_df_pairs
    
    # Merge com LazyFrame
    lazy_df = df_pair1.lazy_df.merge(df_pair2.lazy_df, on="a", how="outer")
    lazy_df.sort_values("a", inplace=True) if hasattr(lazy_df, 'sort_values') and 'inplace' in lazy_df.sort_values.__code__.co_varnames else None
    lazy_result = lazy_df.collect()
    
    # Merge com pandas
    pandas_result = df_pair1.pandas_df.merge(df_pair2.pandas_df, on="a", how="outer")
    pandas_result.sort_values("a", inplace=True)
    pandas_result.reset_index(drop=True, inplace=True)
    
    # Verificações
    assert lazy_result.shape == (2, 3)
    assert sorted(lazy_result.columns.tolist()) == ["a", "b", "d"]
    
    # Verificação de valores (considerando NaN)
    b_values = lazy_result["b"].tolist()
    d_values = lazy_result["d"].tolist()
    a_values = lazy_result["a"].tolist()
    
    assert b_values[0] == 2 or pd.isna(b_values[0]) and a_values[0] == 2
    assert pd.isna(b_values[1]) or pd.isna(d_values[0]) and a_values[1] == 1
    assert d_values[1] == 4 or pd.isna(d_values[1]) and a_values[0] == 1
    assert a_values == [1, 2] or a_values == [2, 1]
    
    # Reordenamos as linhas de acordo com o valor de 'a' para comparação
    lazy_result = lazy_result.sort_values('a').reset_index(drop=True)
    pandas_result = pandas_result.sort_values('a').reset_index(drop=True)
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_result, check_dtype=False)


def test_merge_left(left_merge_df_pairs):
    """Testa o merge left entre dois LazyFrames comparando com pandas"""
    df_pair1, df_pair2 = left_merge_df_pairs
    
    # Merge com LazyFrame
    lazy_df = df_pair1.lazy_df.merge(df_pair2.lazy_df, on="a", how="left")
    lazy_df.sort_values("a", inplace=True) if hasattr(lazy_df, 'sort_values') and 'inplace' in lazy_df.sort_values.__code__.co_varnames else None
    lazy_result = lazy_df.collect()
    
    # Merge com pandas
    pandas_result = df_pair1.pandas_df.merge(df_pair2.pandas_df, on="a", how="left")
    pandas_result.sort_values("a", inplace=True)
    pandas_result.reset_index(drop=True, inplace=True)
    
    # Reordenamos as linhas de acordo com o valor de 'a' para comparação
    lazy_result = lazy_result.sort_values('a').reset_index(drop=True)
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_result, check_dtype=False)


def test_merge_right(left_merge_df_pairs):
    """Testa o merge right entre dois LazyFrames comparando com pandas"""
    df_pair1, df_pair2 = left_merge_df_pairs
    
    # Merge com LazyFrame (invertendo a ordem para simular right join)
    lazy_df = df_pair2.lazy_df.merge(df_pair1.lazy_df, on="a", how="right")
    lazy_df.sort_values("a", inplace=True) if hasattr(lazy_df, 'sort_values') and 'inplace' in lazy_df.sort_values.__code__.co_varnames else None
    lazy_result = lazy_df.collect()
    
    # Merge com pandas
    pandas_result = df_pair2.pandas_df.merge(df_pair1.pandas_df, on="a", how="right")
    pandas_result.sort_values("a", inplace=True)
    pandas_result.reset_index(drop=True, inplace=True)
    
    # Reordenamos as linhas de acordo com o valor de 'a' para comparação
    lazy_result = lazy_result.sort_values('a').reset_index(drop=True)
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_result, check_dtype=False)
