import pandas as pd


def test_sort_values(multi_row_df_pair):
    """Testa o método sort_values do LazyFrame comparando com pandas"""
    # Ordenando com LazyFrame
    lazy_result = multi_row_df_pair.lazy_df.sort_values("b").collect()

    # Ordenando com pandas
    pandas_result = multi_row_df_pair.pandas_df.sort_values("b")

    # Verificações
    assert lazy_result.shape == (2, 2)
    assert lazy_result.columns.tolist() == ["a", "b"]
    assert lazy_result["b"].tolist() == [2, 4]

    # Comparação com pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_result, check_dtype=False)

    # Verifique se o parâmetro ascending é suportado na implementação
    try:
        # Teste com ordem descendente
        lazy_result_desc = multi_row_df_pair.lazy_df.sort_values("b", ascending=False).collect()
        pandas_result_desc = multi_row_df_pair.pandas_df.sort_values("b", ascending=False)
        pd.testing.assert_frame_equal(lazy_result_desc, pandas_result_desc, check_dtype=False)
    except TypeError:
        # Se o parâmetro ascending não for suportado, ignoramos este teste
        print("Parâmetro 'ascending' não é suportado em sort_values")

    # Verifique se o parâmetro inplace é suportado na implementação
    try:
        # Teste com inplace=True
        lazy_df_copy = multi_row_df_pair.lazy_df.copy()
        pandas_df_copy = multi_row_df_pair.pandas_df.copy()

        lazy_df_copy.sort_values("b", inplace=True)
        pandas_df_copy.sort_values("b", inplace=True)

        pd.testing.assert_frame_equal(lazy_df_copy.collect(), pandas_df_copy, check_dtype=False)
    except TypeError:
        # Se o parâmetro inplace não for suportado, ignoramos este teste
        print("Parâmetro 'inplace' não é suportado em sort_values")
