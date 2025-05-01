import duckdb
import pandas as pd
import pytest

from conftest import DataFramePair
from lazy_pandas import LazyFrame


@pytest.fixture
def datetime_df():
    """Fixture que cria um DataFrame com colunas de data/hora para testes"""
    return DataFramePair(query="""
        SELECT cast('2023-05-01' as datetime) AS dt_time
        UNION ALL
        SELECT cast('2024-01-02 15:00:00' as datetime)
    """)


def test_dt_quarter(datetime_df):
    """Testa o método dt.quarter"""
    # Aplicando dt.quarter em LazyFrame
    datetime_df.lazy_df["quarter"] = datetime_df.lazy_df["dt_time"].dt.quarter
    lazy_result = datetime_df.lazy_df.collect()
    
    # Verificações
    assert lazy_result["quarter"].tolist() == [2, 1]
    
    # Aplicando dt.quarter em pandas
    pandas_df = datetime_df.pandas_df.copy()
    pandas_df["quarter"] = pandas_df["dt_time"].dt.quarter
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_dt_month(datetime_df):
    """Testa o método dt.month"""
    # Aplicando dt.month em LazyFrame
    datetime_df.lazy_df["month"] = datetime_df.lazy_df["dt_time"].dt.month
    lazy_result = datetime_df.lazy_df.collect()
    
    # Verificações
    assert lazy_result["month"].tolist() == [5, 1]
    
    # Aplicando dt.month em pandas
    pandas_df = datetime_df.pandas_df.copy()
    pandas_df["month"] = pandas_df["dt_time"].dt.month
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)


def test_dt_day(datetime_df):
    """Testa o método dt.day"""
    # Aplicando dt.day em LazyFrame
    datetime_df.lazy_df["day"] = datetime_df.lazy_df["dt_time"].dt.day
    lazy_result = datetime_df.lazy_df.collect()
    
    # Verificações
    assert lazy_result["day"].tolist() == [1, 2]
    
    # Aplicando dt.day em pandas
    pandas_df = datetime_df.pandas_df.copy()
    pandas_df["day"] = pandas_df["dt_time"].dt.day
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)
