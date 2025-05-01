import duckdb
import pandas as pd
import pytest

from conftest import DataFramePair
from lazy_pandas import LazyFrame


def test_new_column(simple_df_pair):
    """Testa a adição de uma nova coluna ao LazyFrame"""
    # Adicionando coluna no LazyFrame
    simple_df_pair.lazy_df["c"] = 3
    lazy_result = simple_df_pair.lazy_df.collect()

    # Adicionando coluna no pandas DataFrame para comparação
    pandas_df = simple_df_pair.pandas_df.copy()
    pandas_df["c"] = 3

    # Verificações
    assert lazy_result.shape == (1, 3)
    assert lazy_result.columns.tolist() == ["a", "b", "c"]
    assert lazy_result["c"].tolist() == [3]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_overwrite_column(simple_df_pair):
    """Testa a sobrescrita de uma coluna existente no LazyFrame"""
    # Sobrescrevendo coluna no LazyFrame
    simple_df_pair.lazy_df["a"] = 3
    lazy_result = simple_df_pair.lazy_df.collect()

    # Sobrescrevendo coluna no pandas DataFrame para comparação
    pandas_df = simple_df_pair.pandas_df.copy()
    pandas_df["a"] = 3

    # Verificações
    assert lazy_result.shape == (1, 2)
    assert lazy_result.columns.tolist() == ["a", "b"]
    assert lazy_result["a"].tolist() == [3]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_select_columns(simple_df_pair):
    """Testa a seleção de colunas no LazyFrame"""
    # Selecionando coluna no LazyFrame
    lazy_result = simple_df_pair.lazy_df[["b"]].collect()

    # Selecionando coluna no pandas DataFrame para comparação
    pandas_result = simple_df_pair.pandas_df[["b"]]

    # Verificações
    assert lazy_result.shape == (1, 1)
    assert lazy_result.columns.tolist() == ["b"]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_result, check_dtype=False)
