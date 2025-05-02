import pandas as pd
import pytest

from conftest import DataFramePair


@pytest.fixture
def numeric_column_df():
    """Fixture que cria um DataFrame com colunas numéricas para testes de operações"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4")


def test_negative_value(numeric_column_df):
    """Testa a operação de negação em uma coluna"""
    # Aplicando negação em LazyFrame
    numeric_column_df.lazy_df["a"] = -numeric_column_df.lazy_df["a"]
    lazy_result = numeric_column_df.lazy_df.collect()

    # Aplicando negação em pandas para comparação
    pandas_df = numeric_column_df.pandas_df.copy()
    pandas_df["a"] = -pandas_df["a"]

    # Verificações
    assert lazy_result["a"].tolist() == [-1, -3]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_addition(numeric_column_df):
    """Testa a operação de adição entre colunas"""
    # Adição entre colunas em LazyFrame
    numeric_column_df.lazy_df["c"] = numeric_column_df.lazy_df["a"] + numeric_column_df.lazy_df["b"]
    lazy_result = numeric_column_df.lazy_df.collect()

    # Adição entre colunas em pandas para comparação
    pandas_df = numeric_column_df.pandas_df.copy()
    pandas_df["c"] = pandas_df["a"] + pandas_df["b"]

    # Verificações
    assert lazy_result["c"].tolist() == [3, 7]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_subtraction(numeric_column_df):
    """Testa a operação de subtração entre colunas"""
    # Subtração entre colunas em LazyFrame
    numeric_column_df.lazy_df["c"] = numeric_column_df.lazy_df["a"] - numeric_column_df.lazy_df["b"]
    lazy_result = numeric_column_df.lazy_df.collect()

    # Subtração entre colunas em pandas para comparação
    pandas_df = numeric_column_df.pandas_df.copy()
    pandas_df["c"] = pandas_df["a"] - pandas_df["b"]

    # Verificações
    assert lazy_result["c"].tolist() == [-1, -1]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_multiplication(numeric_column_df):
    """Testa a operação de multiplicação entre colunas"""
    # Multiplicação entre colunas em LazyFrame
    numeric_column_df.lazy_df["c"] = numeric_column_df.lazy_df["a"] * numeric_column_df.lazy_df["b"]
    lazy_result = numeric_column_df.lazy_df.collect()

    # Multiplicação entre colunas em pandas para comparação
    pandas_df = numeric_column_df.pandas_df.copy()
    pandas_df["c"] = pandas_df["a"] * pandas_df["b"]

    # Verificações
    assert lazy_result["c"].tolist() == [2, 12]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)
