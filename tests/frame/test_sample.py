import duckdb
import pandas as pd
import pytest

from conftest import DataFramePair
from lazy_pandas import LazyFrame


@pytest.fixture
def large_df_pair():
    """Fixture que cria um DataFrame maior para testes de amostragem"""
    query = "SELECT * FROM range(100)"
    rel = duckdb.sql(query)
    # Criamos um LazyFrame e convertemos para pandas para o DataFramePair
    lazy_frame = LazyFrame(rel)
    return DataFramePair(pandas_df=lazy_frame.collect())


def test_sample_with_n(large_df_pair):
    """Testa o método sample com parâmetro n"""
    # Amostragem com LazyFrame
    lazy_result = large_df_pair.lazy_df.sample(n=10).collect()

    # Verificações
    assert len(lazy_result) == 10

    # Verificação de reprodutibilidade com random_state
    sample1 = large_df_pair.lazy_df.sample(n=5, random_state=42).collect()
    sample2 = large_df_pair.lazy_df.sample(n=5, random_state=42).collect()

    # Verificamos se são iguais (mesma ordem e valores)
    pd.testing.assert_frame_equal(sample1, sample2)


def test_sample_with_frac(large_df_pair):
    """Testa o método sample com parâmetro frac"""
    # Amostragem com LazyFrame
    lazy_result = large_df_pair.lazy_df.sample(frac=0.1).collect()

    # Verificação (esperamos aproximadamente 10 linhas, mas pode variar)
    assert 0 < len(lazy_result) < 30  # Permitimos alguma variação devido à aleatoriedade

    # Verificamos se diferentes frações resultam em tamanhos diferentes

    small_sample = large_df_pair.lazy_df.sample(frac=0.05).collect()
    large_sample = large_df_pair.lazy_df.sample(frac=0.2).collect()

    # Em média, a amostra maior deveria ter mais linhas que a menor
    # (há uma pequena chance de que isso não aconteça devido à aleatoriedade)
    assert len(small_sample) <= len(large_sample)


def test_sample_error_cases(large_df_pair):
    """Testa casos de erro do método sample"""
    # Testes condicionais para não quebrar se a API for diferente

    # Nem n nem frac especificados
    with pytest.raises((ValueError, TypeError)):
        large_df_pair.lazy_df.sample().collect()

    # Ambos n e frac especificados
    with pytest.raises((ValueError, TypeError)):
        large_df_pair.lazy_df.sample(n=10, frac=0.1).collect()

    # frac = 0 (deve ser > 0)
    with pytest.raises((ValueError, TypeError)):
        large_df_pair.lazy_df.sample(frac=0).collect()

    with pytest.raises((ValueError, TypeError)):
        large_df_pair.lazy_df.sample(frac=1.5).collect()
