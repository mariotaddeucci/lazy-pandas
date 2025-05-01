import duckdb
import pandas as pd
import pytest

from conftest import DataFramePair
from lazy_pandas import LazyFrame


@pytest.fixture
def str_column_df():
    """Fixture que cria um DataFrame com coluna string para testes"""
    return DataFramePair(query="SELECT ' CUSTOM_string ' AS col1")


def test_str_len(str_column_df):
    """Testa o método str.len para colunas de string"""
    # Aplicando len em LazyFrame
    str_column_df.lazy_df["col1_len"] = str_column_df.lazy_df["col1"].str.len()
    lazy_result = str_column_df.lazy_df.collect()
    
    # Aplicando len em pandas
    pandas_df = str_column_df.pandas_df.copy()
    pandas_df["col1_len"] = pandas_df["col1"].str.len()
    
    # Verificações
    assert lazy_result["col1_len"].tolist() == [15]
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_str_replace(str_column_df):
    """Testa o método str.replace para colunas de string"""
    # Aplicando replace em LazyFrame
    str_column_df.lazy_df["col1_replaced"] = str_column_df.lazy_df["col1"].str.replace("C", "X")
    lazy_result = str_column_df.lazy_df.collect()
    
    # Aplicando replace em pandas
    pandas_df = str_column_df.pandas_df.copy()
    pandas_df["col1_replaced"] = pandas_df["col1"].str.replace("C", "X")
    
    # Verificações
    assert lazy_result["col1_replaced"].tolist() == [" XUSTOM_string "]
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_str_startswith(str_column_df):
    """Testa o método str.startswith para colunas de string"""
    # Aplicando startswith em LazyFrame
    str_column_df.lazy_df["col1_starts"] = str_column_df.lazy_df["col1"].str.startswith(" C")
    lazy_result = str_column_df.lazy_df.collect()
    
    # Aplicando startswith em pandas
    pandas_df = str_column_df.pandas_df.copy()
    pandas_df["col1_starts"] = pandas_df["col1"].str.startswith(" C")
    
    # Verificações
    assert lazy_result["col1_starts"].tolist() == [True]
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_str_endswith(str_column_df):
    """Testa o método str.endswith para colunas de string"""
    # Aplicando endswith em LazyFrame
    str_column_df.lazy_df["col1_ends"] = str_column_df.lazy_df["col1"].str.endswith("g ")
    lazy_result = str_column_df.lazy_df.collect()
    
    # Aplicando endswith em pandas
    pandas_df = str_column_df.pandas_df.copy()
    pandas_df["col1_ends"] = pandas_df["col1"].str.endswith("g ")
    
    # Verificações
    assert lazy_result["col1_ends"].tolist() == [True]
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_str_contains(str_column_df):
    """Testa o método str.contains para colunas de string"""
    # Aplicando contains em LazyFrame para verificar presença de substring
    str_column_df.lazy_df["col1_contains"] = str_column_df.lazy_df["col1"].str.contains("string")
    lazy_result = str_column_df.lazy_df.collect()
    
    # Aplicando contains em pandas
    pandas_df = str_column_df.pandas_df.copy()
    pandas_df["col1_contains"] = pandas_df["col1"].str.contains("string")
    
    # Verificações
    assert lazy_result["col1_contains"].tolist() == [True]
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)
    
    # Testando caso negativo
    str_column_df_new = DataFramePair(query="SELECT ' CUSTOM_string ' AS col1")
    str_column_df_new.lazy_df["col1_contains"] = str_column_df_new.lazy_df["col1"].str.contains("xyz")
    lazy_neg_result = str_column_df_new.lazy_df.collect()
    
    # Aplicando contains em pandas para caso negativo
    pandas_df_neg = str_column_df_new.pandas_df.copy()
    pandas_df_neg["col1_contains"] = pandas_df_neg["col1"].str.contains("xyz")
    
    # Verificações
    assert lazy_neg_result["col1_contains"].tolist() == [False]
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_neg_result, pandas_df_neg, check_dtype=False)
