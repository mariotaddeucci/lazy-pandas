import pandas as pd
import pytest

from lazy_pandas import LazyFrame


def test_list_columns(simple_df_pair):
    """Testa a listagem de colunas no LazyFrame comparando com pandas"""
    # Verificação da função columns do LazyFrame
    assert simple_df_pair.lazy_df.columns == ["a", "b"]
    # Verificação do tipo dos nomes das colunas
    for col_name in simple_df_pair.lazy_df.columns:
        assert isinstance(col_name, str)

    # Comparação com pandas
    assert simple_df_pair.lazy_df.columns == simple_df_pair.pandas_df.columns.tolist()
