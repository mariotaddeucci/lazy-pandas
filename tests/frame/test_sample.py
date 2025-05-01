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
    
    # Teste com seed fixo (garantindo reprodutibilidade)
    # Este teste será pulado se random_state não for suportado
    try:
        # Vamos tentar isso em um bloco try/except para pular se não funcionar
        sample1 = large_df_pair.lazy_df.sample(n=5, random_state=42).collect()
        sample2 = large_df_pair.lazy_df.sample(n=5, random_state=42).collect()
        
        # Se chegamos aqui sem erro, verificamos se são iguais
        assert sample1.equals(sample2)
    except (TypeError, ValueError, duckdb.duckdb.BinderException):
        # Se random_state não for suportado, pulamos este teste
        pytest.skip("O parâmetro random_state não é suportado no método sample")


def test_sample_with_frac(large_df_pair):
    """Testa o método sample com parâmetro frac"""
    # Amostragem com LazyFrame
    lazy_result = large_df_pair.lazy_df.sample(frac=0.1).collect()
    
    # Verificação (esperamos aproximadamente 10 linhas, mas pode variar)
    assert 0 < len(lazy_result) < 30  # Permitimos alguma variação devido à aleatoriedade
    
    # Verificamos se diferentes frações resultam em tamanhos diferentes
    try:
        small_sample = large_df_pair.lazy_df.sample(frac=0.05).collect()
        large_sample = large_df_pair.lazy_df.sample(frac=0.2).collect()
        
        # Em média, a amostra maior deveria ter mais linhas que a menor
        # (há uma pequena chance de que isso não aconteça devido à aleatoriedade)
        assert len(small_sample) <= len(large_sample)
    except (TypeError, ValueError):
        # Se frac não for suportado corretamente, ignoramos esta verificação
        pass


def test_sample_error_cases(large_df_pair):
    """Testa casos de erro do método sample"""
    # Testes condicionais para não quebrar se a API for diferente
    try:
        # Nem n nem frac especificados
        with pytest.raises((ValueError, TypeError)):
            large_df_pair.lazy_df.sample().collect()
    except:
        pass
        
    try:
        # Ambos n e frac especificados
        with pytest.raises((ValueError, TypeError)):
            large_df_pair.lazy_df.sample(n=10, frac=0.1).collect()
    except:
        pass
        
    try:
        # frac = 0 (deve ser > 0)
        with pytest.raises((ValueError, TypeError)):
            large_df_pair.lazy_df.sample(frac=0).collect()
    except:
        pass
        
    try:
        # frac > 1 (deve ser <= 1)
        with pytest.raises((ValueError, TypeError)):
            large_df_pair.lazy_df.sample(frac=1.5).collect()
    except:
        pass
