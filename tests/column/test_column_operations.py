import duckdb
import pandas as pd
import pytest

from conftest import DataFramePair
from lazy_pandas import LazyFrame


@pytest.fixture
def numeric_column_df():
    """Fixture que cria um DataFrame com colunas numéricas para testes de operações"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4")


def test_division(numeric_column_df):
    """Testa a operação de divisão entre colunas"""
    # Divisão entre colunas em LazyFrame
    numeric_column_df.lazy_df["c"] = numeric_column_df.lazy_df["a"] / numeric_column_df.lazy_df["b"]
    lazy_result = numeric_column_df.lazy_df.collect()

    # Divisão entre colunas em pandas para comparação
    pandas_df = numeric_column_df.pandas_df.copy()
    pandas_df["c"] = pandas_df["a"] / pandas_df["b"]

    # Verificações
    assert lazy_result["c"].tolist() == [0.5, 0.75]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_addition_constant(numeric_column_df):
    """Testa a operação de adição com constante"""
    # Adição de constante em LazyFrame
    numeric_column_df.lazy_df["c"] = numeric_column_df.lazy_df["a"] + 2
    lazy_result = numeric_column_df.lazy_df.collect()

    # Adição de constante em pandas para comparação
    pandas_df = numeric_column_df.pandas_df.copy()
    pandas_df["c"] = pandas_df["a"] + 2

    # Verificações
    assert lazy_result["c"].tolist() == [3, 5]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_cast_column(numeric_column_df):
    """Testa a conversão de tipos de coluna (cast)"""
    # Conversão para string em LazyFrame
    numeric_column_df.lazy_df["a"] = numeric_column_df.lazy_df["a"].astype(str)
    lazy_result_str = numeric_column_df.lazy_df.collect()

    # Conversão para string em pandas
    pandas_df = numeric_column_df.pandas_df.copy()
    pandas_df["a"] = pandas_df["a"].astype(str)

    # Verificações
    assert lazy_result_str["a"].tolist() == ["1", "3"]
    pd.testing.assert_frame_equal(lazy_result_str, pandas_df, check_dtype=False)

    # Conversão de volta para int
    numeric_column_df.lazy_df["a"] = numeric_column_df.lazy_df["a"].astype(int)
    lazy_result_int = numeric_column_df.lazy_df.collect()

    # Conversão para int em pandas
    pandas_df["a"] = pandas_df["a"].astype(int)

    # Verificações
    assert lazy_result_int["a"].tolist() == [1, 3]
    pd.testing.assert_frame_equal(lazy_result_int, pandas_df, check_dtype=False)

    # Conversão para float
    numeric_column_df.lazy_df["a"] = numeric_column_df.lazy_df["a"].astype(float)
    lazy_result_float = numeric_column_df.lazy_df.collect()

    # Conversão para float em pandas
    pandas_df["a"] = pandas_df["a"].astype(float)

    # Verificações
    assert lazy_result_float["a"].tolist() == [1.0, 3.0]
    pd.testing.assert_frame_equal(lazy_result_float, pandas_df, check_dtype=False)
