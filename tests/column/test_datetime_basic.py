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


def test_dt_date(datetime_df):
    """Testa o método dt.date"""
    # Aplicando dt.date em LazyFrame
    datetime_df.lazy_df["dt_time"] = datetime_df.lazy_df["dt_time"].dt.date
    lazy_result = datetime_df.lazy_df.collect()
    
    # Verificações
    assert lazy_result["dt_time"].tolist() == [pd.Timestamp(2023, 5, 1), pd.Timestamp(2024, 1, 2)]
    
    # Aplicando dt.date em pandas
    pandas_df = datetime_df.pandas_df.copy()
    pandas_df["dt_time"] = pandas_df["dt_time"].dt.date
    
    # Comparação com pandas (convertendo para timestamp para comparação)
    pandas_result = pandas_df.copy()
    pandas_result["dt_time"] = pandas_result["dt_time"].apply(lambda x: pd.Timestamp(x))
    pd.testing.assert_frame_equal(lazy_result, pandas_result, check_dtype=False)


def test_dt_year(datetime_df):
    """Testa o método dt.year"""
    # Aplicando dt.year em LazyFrame
    datetime_df.lazy_df["year"] = datetime_df.lazy_df["dt_time"].dt.year
    lazy_result = datetime_df.lazy_df.collect()
    
    # Verificações
    assert lazy_result["year"].tolist() == [2023, 2024]
    
    # Aplicando dt.year em pandas
    pandas_df = datetime_df.pandas_df.copy()
    pandas_df["year"] = pandas_df["dt_time"].dt.year
    
    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_df, check_dtype=False)
