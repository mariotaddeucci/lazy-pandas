def test_list_columns(simple_df_pair):
    """Tests the listing of columns in LazyFrame compared to pandas"""
    # Verification of the columns function in LazyFrame
    assert simple_df_pair.lazy_df.columns == ["a", "b"]
    # Verification of column names type
    for col_name in simple_df_pair.lazy_df.columns:
        assert isinstance(col_name, str)

    # Comparison with pandas
    assert simple_df_pair.lazy_df.columns == simple_df_pair.pandas_df.columns.tolist()
