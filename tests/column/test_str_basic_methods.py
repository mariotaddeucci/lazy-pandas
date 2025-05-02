import pandas as pd
import pytest

from conftest import DataFramePair


@pytest.fixture
def str_column_df():
    """Fixture que cria um DataFrame com coluna string para testes"""
    return DataFramePair(query="SELECT ' CUSTOM_string ' AS col1")


def test_str_lower(str_column_df):
    """Testa o método str.lower para colunas de string"""
    # Aplicando lower em LazyFrame
    str_column_df.lazy_df["col1"] = str_column_df.lazy_df["col1"].str.lower()
    lazy_result = str_column_df.lazy_df.collect()

    # Aplicando lower em pandas
    pandas_df = str_column_df.pandas_df.copy()
    pandas_df["col1"] = pandas_df["col1"].str.lower()

    # Verificações
    assert lazy_result["col1"].tolist() == [" custom_string "]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_str_upper(str_column_df):
    """Testa o método str.upper para colunas de string"""
    # Aplicando upper em LazyFrame
    str_column_df.lazy_df["col1"] = str_column_df.lazy_df["col1"].str.upper()
    lazy_result = str_column_df.lazy_df.collect()

    # Aplicando upper em pandas
    pandas_df = str_column_df.pandas_df.copy()
    pandas_df["col1"] = pandas_df["col1"].str.upper()

    # Verificações
    assert lazy_result["col1"].tolist() == [" CUSTOM_STRING "]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_str_strip(str_column_df):
    """Testa o método str.strip para colunas de string"""
    # Aplicando strip em LazyFrame
    str_column_df.lazy_df["col1"] = str_column_df.lazy_df["col1"].str.strip()
    lazy_result = str_column_df.lazy_df.collect()

    # Aplicando strip em pandas
    pandas_df = str_column_df.pandas_df.copy()
    pandas_df["col1"] = pandas_df["col1"].str.strip()

    # Verificações
    assert lazy_result["col1"].tolist() == ["CUSTOM_string"]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_str_lstrip(str_column_df):
    """Testa o método str.lstrip para colunas de string"""
    # Aplicando lstrip em LazyFrame
    str_column_df.lazy_df["col1"] = str_column_df.lazy_df["col1"].str.lstrip()
    lazy_result = str_column_df.lazy_df.collect()

    # Aplicando lstrip em pandas
    pandas_df = str_column_df.pandas_df.copy()
    pandas_df["col1"] = pandas_df["col1"].str.lstrip()

    # Verificações
    assert lazy_result["col1"].tolist() == ["CUSTOM_string "]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_str_rstrip(str_column_df):
    """Testa o método str.rstrip para colunas de string"""
    # Aplicando rstrip em LazyFrame
    str_column_df.lazy_df["col1"] = str_column_df.lazy_df["col1"].str.rstrip()
    lazy_result = str_column_df.lazy_df.collect()

    # Aplicando rstrip em pandas
    pandas_df = str_column_df.pandas_df.copy()
    pandas_df["col1"] = pandas_df["col1"].str.rstrip()

    # Verificações
    assert lazy_result["col1"].tolist() == [" CUSTOM_string"]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)
