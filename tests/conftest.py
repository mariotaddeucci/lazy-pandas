import duckdb
import numpy as np
import pandas as pd
import pytest

import lazy_pandas as lp
from lazy_pandas import LazyFrame


class DataFramePair:
    """
    Classe utilitária para testar LazyFrame vs pandas DataFrame

    Facilita a execução dos mesmos testes em LazyFrame e pandas DataFrame,
    permitindo comparar os resultados para garantir equivalência.
    """

    def __init__(self, query=None, pandas_df=None, lazy_df=None):
        """
        Inicializa um par de DataFrames (LazyFrame e pandas) para testes

        Args:
            query: Consulta SQL para criar os dataframes
            pandas_df: DataFrame pandas já existente para usar como base
            lazy_df: LazyFrame já existente para usar como base
        """
        if query:
            self.rel = duckdb.sql(query)
            self.lazy_df = LazyFrame(self.rel)
            self.pandas_df = self.lazy_df.collect()
        elif pandas_df is not None:
            self.pandas_df = pandas_df
            # Converter pandas para DuckDB e depois para LazyFrame
            self.lazy_df = LazyFrame(duckdb.from_df(pandas_df))
        elif lazy_df is not None:
            self.lazy_df = lazy_df
            self.pandas_df = lazy_df.collect()
        else:
            raise ValueError("Deve fornecer query, pandas_df ou lazy_df")

    def copy(self):
        """Retorna uma cópia do par de DataFrames"""
        result = DataFramePair.__new__(DataFramePair)
        result.lazy_df = self.lazy_df.copy() if hasattr(self, 'lazy_df') else None
        result.pandas_df = self.pandas_df.copy() if hasattr(self, 'pandas_df') else None
        return result

    def assert_equal(self, pandas_result, lazy_result):
        """
        Verifica se os resultados de pandas e lazy_pandas são equivalentes.
        """
        if isinstance(pandas_result, pd.DataFrame) and isinstance(lazy_result, pd.DataFrame):
            pd.testing.assert_frame_equal(pandas_result, lazy_result, check_dtype=False)
        elif isinstance(pandas_result, pd.Series) and isinstance(lazy_result, pd.Series):
            pd.testing.assert_series_equal(pandas_result, lazy_result, check_dtype=False)
        elif isinstance(pandas_result, list) and isinstance(lazy_result, list):
            assert pandas_result == lazy_result
        elif isinstance(pandas_result, tuple) and isinstance(lazy_result, tuple):
            assert pandas_result == lazy_result
        elif np.isscalar(pandas_result) and np.isscalar(lazy_result):
            if pd.isna(pandas_result) and pd.isna(lazy_result):
                assert True
            else:
                assert pandas_result == lazy_result
        else:
            assert pandas_result == lazy_result


@pytest.fixture
def int_column_df():
    """Fixture que retorna um DataFrame com colunas de inteiros para testes"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4")


@pytest.fixture
def multi_row_df():
    """Fixture que retorna um DataFrame com múltiplas linhas para testes"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4 UNION ALL SELECT 5, 6")


@pytest.fixture
def datetime_df():
    """Fixture que retorna um DataFrame com colunas datetime para testes"""
    return DataFramePair(
        query="""
        SELECT cast('2023-05-01' as datetime) AS dt_time
        UNION ALL
        SELECT cast('2024-01-02 15:00:00' as datetime)
    """
    )


@pytest.fixture
def simple_df_pair():
    """Fixture que cria um par simples DataFrame/LazyFrame"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b")


@pytest.fixture
def multi_row_df_pair():
    """Fixture que cria um par DataFrame/LazyFrame com múltiplas linhas"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4")


@pytest.fixture
def numeric_df_pair():
    """Fixture que cria um par DataFrame/LazyFrame com dados numéricos"""
    return DataFramePair(
        query="""
        SELECT 1 AS a, 2 AS b, 3 AS c
        UNION ALL SELECT 4, 5, 6
        UNION ALL SELECT 7, 8, 9
    """
    )


@pytest.fixture
def duplicate_df_pair():
    """Fixture que cria um par DataFrame/LazyFrame com linhas duplicadas"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b UNION ALL SELECT 1, 2")


@pytest.fixture
def partial_duplicate_df_pair():
    """Fixture que cria um par DataFrame/LazyFrame com duplicação parcial"""
    return DataFramePair(query="SELECT 1 AS a, 2 AS b UNION ALL SELECT 2, 2")
