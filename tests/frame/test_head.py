import pandas as pd


def test_head(multi_row_df_pair):
    """Tests the head method of LazyFrame compared to pandas"""
    # Getting first rows with LazyFrame
    lazy_result = multi_row_df_pair.lazy_df.head(1).collect()

    # Getting first rows with pandas
    pandas_result = multi_row_df_pair.pandas_df.head(1)

    # Verifications
    assert lazy_result.shape == (1, 2)
    assert lazy_result.columns.tolist() == ["a", "b"]

    # Comparison with pandas
    pd.testing.assert_frame_equal(lazy_result, pandas_result)

    # Test with different limit
    lazy_result_full = multi_row_df_pair.lazy_df.head(10).collect()
    pandas_result_full = multi_row_df_pair.pandas_df.head(10)
    pd.testing.assert_frame_equal(lazy_result_full, pandas_result_full)

    # Test without parameter (should use default of 5)
    lazy_result_default = multi_row_df_pair.lazy_df.head().collect()
    pandas_result_default = multi_row_df_pair.pandas_df.head()
    pd.testing.assert_frame_equal(lazy_result_default, pandas_result_default)
