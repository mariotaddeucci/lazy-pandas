import os
import tempfile
from io import StringIO

import pandas as pd
import pytest

import lazy_pandas as lp
from lazy_pandas import LazyFrame


def test_from_pandas():
    """Testa a conversão de um DataFrame pandas para LazyFrame."""
    # Criar um DataFrame do pandas
    pandas_df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    # Converter para LazyFrame
    lazy_df = lp.from_pandas(pandas_df)

    # Verificações
    assert isinstance(lazy_df, LazyFrame)
    result = lazy_df.collect()
    pd.testing.assert_frame_equal(result, pandas_df)


def test_read_csv_from_buffer():
    """Testa a leitura de CSV a partir de um buffer (StringIO)."""
    # Criar um buffer com dados CSV
    csv_data = "col1,col2\n1,a\n2,b\n3,c"
    buffer = StringIO(csv_data)

    # Ler do buffer com diferentes opções
    lazy_df = lp.read_csv(buffer, header=True, sep=",")

    # Verificações
    assert isinstance(lazy_df, LazyFrame)
    result = lazy_df.collect()
    assert result.shape == (3, 2)
    assert list(result.columns) == ["col1", "col2"]

    # Valores são convertidos para inteiros automaticamente, então ajustamos o teste
    assert result["col1"].tolist() == [1, 2, 3]
    assert result["col2"].tolist() == ["a", "b", "c"]


def test_read_csv_with_options():
    """Testa a leitura de CSV com várias opções."""
    # Criar um arquivo temporário com dados CSV
    with tempfile.NamedTemporaryFile(suffix=".csv", mode="w+", delete=False) as f:
        f.write("col1;col2;col3\n1;a;2020-01-01\n2;b;2020-01-02\n3;c;2020-01-03")
        temp_path = f.name

    try:
        # Testar com diferentes opções
        lazy_df = lp.read_csv(
            temp_path, sep=";", header=True, parse_dates=["col3"], all_varchar=True, normalize_names=True
        )

        # Verificações
        assert isinstance(lazy_df, LazyFrame)
        result = lazy_df.collect()
        assert result.shape == (3, 3)
        assert list(result.columns) == ["col1", "col2", "col3"]

        # Verificar se col3 foi analisado corretamente como data
        assert pd.api.types.is_datetime64_dtype(result["col3"].dtype)
    finally:
        # Limpar arquivo temporário
        os.unlink(temp_path)


def test_read_parquet():
    """Testa a leitura de arquivos Parquet."""
    # Precisamos de um arquivo parquet para testar
    # Vamos criar um dataframe e salvá-lo como parquet em um arquivo temporário
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [4.1, 5.2, 6.3]})

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        temp_path = f.name

    try:
        # Salvar como parquet
        df.to_parquet(temp_path)

        # Testar leitura completa
        lazy_df = lp.read_parquet(temp_path)
        assert isinstance(lazy_df, LazyFrame)
        result = lazy_df.collect()
        pd.testing.assert_frame_equal(result, df)

        # Testar leitura de colunas específicas
        lazy_df_cols = lp.read_parquet(temp_path, columns=["a", "c"])
        result_cols = lazy_df_cols.collect()
        assert list(result_cols.columns) == ["a", "c"]
        pd.testing.assert_frame_equal(result_cols, df[["a", "c"]])

    finally:
        # Limpar arquivo temporário
        os.unlink(temp_path)


@pytest.mark.skipif(
    not os.path.exists(os.path.join(os.path.dirname(__file__), "assets/delta_table")),
    reason="Arquivo de teste delta_table não encontrado",
)
def test_read_delta():
    """Testa a leitura de tabelas Delta Lake."""
    # Usa o arquivo de exemplo delta_table em /tests/assets/ se existir
    delta_dir = os.path.join(os.path.dirname(__file__), "assets/delta_table")

    if os.path.exists(delta_dir):
        # Ler a tabela Delta
        lazy_df = lp.read_delta(delta_dir)

        # Verificações
        assert isinstance(lazy_df, LazyFrame)
        result = lazy_df.collect()
        assert isinstance(result, pd.DataFrame)
        # Verificamos que podemos coletar o resultado sem erros
        # As colunas específicas dependem do conteúdo da tabela Delta de teste


@pytest.mark.skipif(True, reason="Depende de configuração específica do Iceberg")
def test_read_iceberg():
    """
    Testa a leitura de tabelas Apache Iceberg.
    Este teste é desativado por padrão porque depende de uma tabela Iceberg válida.
    """
    # Este teste só funcionará se tivermos uma tabela Iceberg válida para testar
    # Usa o fixture iceberg_table_uri do arquivo reader_test.py se disponível
    from tests.reader_test import iceberg_table_uri

    table_path = iceberg_table_uri()

    # Ler a tabela Iceberg
    lazy_df = lp.read_iceberg(table_path)

    # Verificações
    assert isinstance(lazy_df, LazyFrame)
    result = lazy_df.collect()
    assert isinstance(result, pd.DataFrame)
    assert "lat" in result.columns
    assert "long" in result.columns
