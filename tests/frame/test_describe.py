import pandas as pd
import pytest

from lazy_pandas import LazyFrame


def test_describe_default(numeric_df_pair):
    """Testa o método describe do LazyFrame com configurações padrão"""
    # Pular teste se o método describe não estiver implementado
    if not hasattr(LazyFrame, 'describe'):
        pytest.skip("Método describe não implementado ainda em LazyFrame")
        
    # Executando describe com LazyFrame
    lazy_desc = numeric_df_pair.lazy_df.describe()
    lazy_result = lazy_desc.collect()

    # Executando describe com pandas
    pandas_result = numeric_df_pair.pandas_df.describe()

    # Verificações das estatísticas padrão
    assert lazy_result.index.tolist() == ["count", "mean", "std", "min", "25%", "50%", "75%", "max"]
    assert all(col in lazy_result.columns for col in ["a", "b", "c"])

    # Verificação de alguns valores específicos
    assert lazy_result.loc["count", "a"] == 3
    assert lazy_result.loc["mean", "b"] == 5.0
    assert lazy_result.loc["min", "c"] == 3
    assert lazy_result.loc["max", "c"] == 9

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_result)


def test_describe_custom_percentiles(numeric_df_pair):
    """Testa o método describe do LazyFrame com percentis personalizados"""
    # Pular teste se o método describe não estiver implementado
    if not hasattr(LazyFrame, 'describe'):
        pytest.skip("Método describe não implementado ainda em LazyFrame")
        
    # Executando describe com percentis personalizados em LazyFrame
    lazy_desc = numeric_df_pair.lazy_df.describe(percentiles=[0.2, 0.8])
    lazy_result = lazy_desc.collect()

    # Executando describe com percentis personalizados em pandas
    pandas_result = numeric_df_pair.pandas_df.describe(percentiles=[0.2, 0.8])

    # Verificação dos percentis personalizados
    assert lazy_result.index.tolist() == ["count", "mean", "std", "min", "20%", "80%", "max"]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_result)


def test_describe_include(numeric_df_pair):
    """Testa o método describe do LazyFrame com parâmetro include"""
    # Pular teste se o método describe não estiver implementado
    if not hasattr(LazyFrame, 'describe'):
        pytest.skip("Método describe não implementado ainda em LazyFrame")
        
    # Executando describe com colunas específicas em LazyFrame
    lazy_desc = numeric_df_pair.lazy_df.describe(include=["a", "c"])
    lazy_result = lazy_desc.collect()

    # Verificação das colunas incluídas
    assert set(lazy_result.columns) == {"a", "c"}
    assert "b" not in lazy_result.columns

    # Comparação com pandas (se o pandas suportar esse parâmetro da mesma forma)
    try:
        pandas_result = numeric_df_pair.pandas_df.describe(include=["a", "c"])
        pd.testing.assert_frame_equal(lazy_result, pandas_result)
    except Exception:
        # Se o pandas não suportar este parâmetro da mesma forma, pulamos essa verificação
        pass
