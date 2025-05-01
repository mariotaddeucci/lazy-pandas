import duckdb
import pandas as pd
import pytest

from conftest import DataFramePair
from lazy_pandas import LazyFrame


@pytest.fixture
def numeric_column_df():
    """Fixture que cria um DataFrame com colunas numéricas para testes de operações"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4")


def test_clip_both_bounds(numeric_column_df):
    """Testa o método clip com ambos os limites (inferior e superior)"""
    # Aplicando clip em LazyFrame
    numeric_column_df.lazy_df["c"] = numeric_column_df.lazy_df["a"].clip(2, 3)
    lazy_result = numeric_column_df.lazy_df.collect()
    
    # Aplicando clip em pandas para comparação
    pandas_df = numeric_column_df.pandas_df.copy()
    pandas_df["c"] = pandas_df["a"].clip(2, 3)
    
    # Verificações
    assert lazy_result["c"].tolist() == [2, 3]
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_clip_lower_bound(numeric_column_df):
    """Testa o método clip apenas com limite inferior"""
    # Aplicando clip em LazyFrame
    numeric_column_df.lazy_df["c"] = numeric_column_df.lazy_df["a"].clip(lower=2)
    lazy_result = numeric_column_df.lazy_df.collect()
    
    # Aplicando clip em pandas para comparação
    pandas_df = numeric_column_df.pandas_df.copy()
    pandas_df["c"] = pandas_df["a"].clip(lower=2)
    
    # Verificações
    assert lazy_result["c"].tolist() == [2, 3]
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_clip_upper_bound(numeric_column_df):
    """Testa o método clip apenas com limite superior"""
    # Aplicando clip em LazyFrame
    numeric_column_df.lazy_df["c"] = numeric_column_df.lazy_df["a"].clip(upper=2)
    lazy_result = numeric_column_df.lazy_df.collect()
    
    # Aplicando clip em pandas para comparação
    pandas_df = numeric_column_df.pandas_df.copy()
    pandas_df["c"] = pandas_df["a"].clip(upper=2)
    
    # Verificações
    assert lazy_result["c"].tolist() == [1, 2]
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_clip_no_bounds(numeric_column_df):
    """Testa o método clip sem limites (deve retornar os mesmos valores)"""
    # Aplicando clip em LazyFrame
    numeric_column_df.lazy_df["c"] = numeric_column_df.lazy_df["a"].clip()
    lazy_result = numeric_column_df.lazy_df.collect()
    
    # Aplicando clip em pandas para comparação
    pandas_df = numeric_column_df.pandas_df.copy()
    pandas_df["c"] = pandas_df["a"].clip()
    
    # Verificações
    assert lazy_result["c"].tolist() == [1, 3]
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)
