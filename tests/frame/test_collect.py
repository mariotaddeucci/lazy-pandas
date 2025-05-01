import pandas as pd
import pytest

from lazy_pandas import LazyFrame


def test_collect(simple_df_pair):
    """Testa o método collect do LazyFrame"""
    # Teste básico de coleta
    df = simple_df_pair.lazy_df.collect()

    # Verificações
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ["a", "b"]

    # Comparação direta com o DataFrame do pandas
    pd.testing.assert_frame_equal(df, simple_df_pair.pandas_df)
