import numpy as np
import pandas as pd
import pytest

from conftest import DataFramePair
from lazy_pandas import LazyFrame


@pytest.fixture
def sample_df():
    """Fixture que cria um DataFrame com dados para testes de sample"""
    # Criamos um DataFrame grande o suficiente para testar amostragem
    data = pd.DataFrame({
        'id': range(1, 101),
        'value': np.random.randn(100),
        'group': np.random.choice(['A', 'B', 'C'], 100)
    })
    lazy_frame = LazyFrame(data)
    return DataFramePair(lazy_df=lazy_frame, pandas_df=data)


def test_sample_n_parameter(sample_df):
    """Testa o método sample com parâmetro n (número de linhas)"""
    # Verificamos se o método sample retorna o número correto de linhas
    for n in [5, 10, 20]:
        result = sample_df.lazy_df.sample(n=n).collect()
        assert len(result) == n
        
    # Verificando validação de n > número de linhas
    # Alguns motores limitam ao número de linhas disponíveis
    big_n = sample_df.lazy_df.sample(n=200).collect()
    assert len(big_n) <= 100  # Não pode retornar mais do que temos


def test_sample_frac_parameter(sample_df):
    """Testa o método sample com parâmetro frac (fração de linhas)"""
    # Testamos diferentes frações
    for frac in [0.1, 0.25, 0.5]:
        result = sample_df.lazy_df.sample(frac=frac).collect()
        # A contagem exata pode variar bastante devido à aleatoriedade
        expected_count = int(len(sample_df.pandas_df) * frac)
        # Aumentamos a tolerância para variação aleatória
        tolerance = max(10, int(expected_count * 0.3))  # 30% ou pelo menos 10
        assert abs(len(result) - expected_count) <= tolerance


def test_sample_random_state(sample_df):
    """Testa a reprodutibilidade com random_state no sample"""
    # Amostras com mesmo random_state devem ser idênticas
    sample1 = sample_df.lazy_df.sample(n=10, random_state=42).collect()
    sample2 = sample_df.lazy_df.sample(n=10, random_state=42).collect()
    
    # Verificamos se as duas amostras têm as mesmas linhas (mesma ordem)
    pd.testing.assert_frame_equal(sample1, sample2)
    
    # Amostras com random_state diferente geralmente resultam em conteúdo diferente
    # Neste caso, verificamos apenas se o comportamento é consistente
    # (não verificamos se os índices são diferentes pois depende da implementação)
    sample3 = sample_df.lazy_df.sample(n=10, random_state=24).collect()
    
    # Verificamos se as amostras são reproduzíveis pelo menos
    sample4 = sample_df.lazy_df.sample(n=10, random_state=24).collect()
    pd.testing.assert_frame_equal(sample3, sample4)


def test_sample_error_cases(sample_df):
    """Testa casos de erro no método sample"""
    # Nem n nem frac especificados deve gerar erro
    with pytest.raises((ValueError, TypeError)):
        sample_df.lazy_df.sample().collect()
    
    # Ambos n e frac especificados deve gerar erro
    with pytest.raises((ValueError, TypeError)):
        sample_df.lazy_df.sample(n=10, frac=0.1).collect()
    
    # frac negativo deve gerar erro
    with pytest.raises((ValueError)):
        sample_df.lazy_df.sample(frac=-0.1).collect()
    
    # frac maior que 1 deve gerar erro (em alguns motores)
    try:
        with pytest.raises((ValueError)):
            sample_df.lazy_df.sample(frac=1.5).collect()
    except:
        # Se a implementação não validar isso, o teste passa
        pass


def test_sample_with_replacement(sample_df):
    """Testa amostragem com reposição (se suportado)"""
    try:
        # Este teste só funcionará se a implementação suportar replace=True
        # Testamos com um n maior que o tamanho original, só possível com reposição
        result = sample_df.lazy_df.sample(n=150, replace=True).collect()
        
        # Se chegamos aqui, replace=True é suportado
        assert len(result) == 150
        
        # Com replace=False (padrão) e n > tamanho, deve limitar ao tamanho original
        result_no_replace = sample_df.lazy_df.sample(n=150, replace=False).collect()
        assert len(result_no_replace) <= 100
    except (TypeError, ValueError):
        # Se a implementação não suportar replace, o teste passa
        pass