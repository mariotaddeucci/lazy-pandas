import pandas as pd
import pytest

from conftest import DataFramePair


@pytest.fixture
def datetime_extended_df():
    """Fixture que cria um DataFrame com uma variedade de datas para testes mais abrangentes"""
    return DataFramePair(
        query="""
        SELECT cast('2023-01-01' as datetime) AS dt_time
        UNION ALL SELECT cast('2023-12-31 00:00:00' as datetime)  -- fim de ano sem parte de tempo
        UNION ALL SELECT cast('2024-02-29 12:30:45' as datetime)  -- ano bissexto
        UNION ALL SELECT cast('2023-03-31 00:00:00' as datetime)  -- fim do mês
        UNION ALL SELECT NULL                                    -- valor nulo
    """
    )


def test_dt_date_parts(datetime_extended_df):
    """Testa diversos componentes de data/hora simultaneamente"""
    # Aplicamos várias operações de extração de componentes de data
    df = datetime_extended_df.lazy_df

    # Ano
    df["year"] = df["dt_time"].dt.year
    # Mês
    df["month"] = df["dt_time"].dt.month
    # Dia
    df["day"] = df["dt_time"].dt.day
    # Hora
    df["hour"] = df["dt_time"].dt.hour

    # Coletamos o resultado
    result = df.collect()

    # Verificações de valores específicos para cada linha
    # Primeira linha: 2023-01-01 00:00:00
    assert result.iloc[0]["year"] == 2023
    assert result.iloc[0]["month"] == 1
    assert result.iloc[0]["day"] == 1
    assert result.iloc[0]["hour"] == 0

    # Segunda linha: 2023-12-31 00:00:00
    assert result.iloc[1]["year"] == 2023
    assert result.iloc[1]["month"] == 12
    assert result.iloc[1]["day"] == 31
    assert result.iloc[1]["hour"] == 0

    # Terceira linha: 2024-02-29 12:30:45 (ano bissexto)
    assert result.iloc[2]["year"] == 2024
    assert result.iloc[2]["month"] == 2
    assert result.iloc[2]["day"] == 29
    assert result.iloc[2]["hour"] == 12


def test_dt_is_special_days(datetime_extended_df):
    """Testa métodos de verificação de dias especiais (início/fim de período)"""
    df = datetime_extended_df.lazy_df

    # Verificando dias especiais
    df["is_month_start"] = df["dt_time"].dt.is_month_start
    df["is_month_end"] = df["dt_time"].dt.is_month_end
    df["is_quarter_start"] = df["dt_time"].dt.is_quarter_start
    df["is_year_start"] = df["dt_time"].dt.is_year_start
    df["is_year_end"] = df["dt_time"].dt.is_year_end

    # Coletamos o resultado
    result = df.collect()

    # Verificações de primeiro dia do mês (01/01/2023)
    assert result.iloc[0]["is_month_start"] is True
    assert result.iloc[0]["is_quarter_start"] is True
    assert result.iloc[0]["is_year_start"] is True

    # Verificações de último dia do mês/ano (31/12/2023)
    # Agora sem a parte de tempo (00:00:00), deve funcionar corretamente
    assert result.iloc[1]["is_month_end"] is True
    assert result.iloc[1]["is_year_end"] is True

    # Verificações de último dia do mês (31/03/2023)
    assert result.iloc[3]["is_month_end"] is True

    # Verificação com valores nulos
    assert pd.isna(result.iloc[4]["is_month_start"])


def test_dt_weekday(datetime_extended_df):
    """Testa método de dia da semana"""
    df = datetime_extended_df.lazy_df

    # Extraindo dia da semana
    df["weekday"] = df["dt_time"].dt.weekday()

    # Coletamos o resultado
    result = df.collect()

    # Na implementação atual, o dia da semana é 0-indexado
    # (0 = domingo, 1 = segunda, ..., 6 = sábado), como no pandas padrão
    # Verificações para 01/01/2023 (deve ser domingo, dia 0)
    assert result.iloc[0]["weekday"] == 0  # domingo = 0

    # Verificações para 31/12/2023 (deve ser domingo)
    assert result.iloc[1]["weekday"] == 0  # domingo = 0

    # Verificações para 29/02/2024 (deve ser quinta)
    assert result.iloc[2]["weekday"] == 4  # quinta = 4
