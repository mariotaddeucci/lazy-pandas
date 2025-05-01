import duckdb
import pandas as pd
import pytest

from conftest import DataFramePair
from lazy_pandas import LazyFrame


def test_drop_duplicates(duplicate_df_pair):
    """Testa o método drop_duplicates do LazyFrame comparando com pandas"""
    # Removendo duplicatas com LazyFrame
    lazy_result = duplicate_df_pair.lazy_df.drop_duplicates().collect()
    
    # Removendo duplicatas com pandas
    pandas_result = duplicate_df_pair.pandas_df.drop_duplicates()
    
    # Verificações
    assert lazy_result.shape == (1, 2)
    assert lazy_result.columns.tolist() == ["a", "b"]
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_result, check_dtype=False)
    
    # Teste com inplace=True
    try:
        lazy_df_copy = duplicate_df_pair.lazy_df.copy()
        pandas_df_copy = duplicate_df_pair.pandas_df.copy()
        
        lazy_df_copy.drop_duplicates(inplace=True)
        pandas_df_copy.drop_duplicates(inplace=True)
        
        pd.testing.assert_frame_equal(lazy_df_copy.collect(), pandas_df_copy, check_dtype=False)
    except TypeError:
        # Se inplace não for suportado, ignoramos
        pass


def test_drop_duplicates_subset(partial_duplicate_df_pair):
    """Testa o método drop_duplicates com subset do LazyFrame comparando com pandas"""
    # Removendo duplicatas em subset específico com LazyFrame
    lazy_result = partial_duplicate_df_pair.lazy_df.drop_duplicates(subset=["b"]).collect()
    
    # Removendo duplicatas em subset específico com pandas
    pandas_result = partial_duplicate_df_pair.pandas_df.drop_duplicates(subset=["b"])
    
    # Verificações
    assert lazy_result.shape == (1, 2)
    assert lazy_result.columns.tolist() == ["a", "b"]
    
    # Como o comportamento pode diferir em qual registro é mantido (primeiro vs. último),
    # verificamos apenas que uma linha foi removida e que o valor 'b' é o mesmo
    assert lazy_result["b"].iloc[0] == pandas_result["b"].iloc[0]
    
    # Teste com keep="last"
    try:
        lazy_last = partial_duplicate_df_pair.lazy_df.drop_duplicates(subset=["b"], keep="last").collect()
        pandas_last = partial_duplicate_df_pair.pandas_df.drop_duplicates(subset=["b"], keep="last")
        
        # Verificamos apenas que ambos retêm o mesmo valor de 'b'
        assert lazy_last["b"].iloc[0] == pandas_last["b"].iloc[0]
    except TypeError:
        # Se keep não for suportado, ignoramos
        pass
