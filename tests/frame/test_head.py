import pandas as pd
import pytest

from lazy_pandas import LazyFrame


def test_head(multi_row_df_pair):
    """Testa o método head do LazyFrame comparando com pandas"""
    # Obtendo primeiras linhas com LazyFrame
    lazy_result = multi_row_df_pair.lazy_df.head(1).collect()

    # Obtendo primeiras linhas com pandas
    pandas_result = multi_row_df_pair.pandas_df.head(1)

    # Verificações
    assert lazy_result.shape == (1, 2)
    assert lazy_result.columns.tolist() == ["a", "b"]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_result)

    # Teste com limite diferente
    lazy_result_full = multi_row_df_pair.lazy_df.head(10).collect()
    pandas_result_full = multi_row_df_pair.pandas_df.head(10)
    pd.testing.assert_frame_equal(lazy_result_full, pandas_result_full)

    # Teste sem parâmetro (deve usar padrão de 5)
    lazy_result_default = multi_row_df_pair.lazy_df.head().collect()
    pandas_result_default = multi_row_df_pair.pandas_df.head()
    pd.testing.assert_frame_equal(lazy_result_default, pandas_result_default)
