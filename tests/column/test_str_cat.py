import pytest

from conftest import DataFramePair


@pytest.fixture
def str_columns_df():
    """Fixture that creates a DataFrame with multiple string columns for concatenation tests"""
    return DataFramePair(
        query="""
        SELECT 
            'First' AS first_name, 
            'Last' AS last_name,
            'Title' AS title
        UNION ALL
        SELECT 
            'John' AS first_name, 
            'Doe' AS last_name,
            'Mr.' AS title
        UNION ALL
        SELECT 
            'Jane' AS first_name, 
            'Smith' AS last_name,
            'Ms.' AS title
        UNION ALL
        SELECT 
            NULL AS first_name, 
            'Brown' AS last_name,
            'Dr.' AS title
        UNION ALL
        SELECT 
            'Alice' AS first_name, 
            NULL AS last_name,
            'Prof.' AS title
        """
    )


def test_str_cat_basic(str_columns_df):
    """Tests the basic string concatenation functionality with the str.cat method"""
    # Applying cat with empty separator in LazyFrame
    str_columns_df.lazy_df["full_name"] = str_columns_df.lazy_df["first_name"].str.cat(
        str_columns_df.lazy_df["last_name"]
    )
    lazy_result = str_columns_df.lazy_df.collect()

    # Verifications for DuckDB behavior (different from pandas)
    # DuckDB's concat_ws ignores NULL values and concatenates what's available
    expected_values = ["FirstLast", "JohnDoe", "JaneSmith", "Brown", "Alice"]
    assert lazy_result["full_name"].tolist() == expected_values


def test_str_cat_with_separator(str_columns_df):
    """Tests the string concatenation with a custom separator using the str.cat method"""
    # Applying cat with space separator in LazyFrame
    str_columns_df.lazy_df["full_name"] = str_columns_df.lazy_df["first_name"].str.cat(
        str_columns_df.lazy_df["last_name"], sep=" "
    )
    lazy_result = str_columns_df.lazy_df.collect()

    # Verifications for DuckDB behavior (different from pandas)
    # DuckDB's concat_ws ignores NULL values and concatenates what's available
    expected_values = ["First Last", "John Doe", "Jane Smith", "Brown", "Alice"]
    assert lazy_result["full_name"].tolist() == expected_values


def test_str_cat_multiple_columns(str_columns_df):
    """Tests concatenating multiple string columns in sequence"""
    # Chaining cat operations to concatenate three columns
    str_columns_df.lazy_df["formatted_name"] = (
        str_columns_df.lazy_df["title"]
        .str.cat(str_columns_df.lazy_df["first_name"], sep=" ")
        .str.cat(str_columns_df.lazy_df["last_name"], sep=" ")
    )
    lazy_result = str_columns_df.lazy_df.collect()

    # Verifications for DuckDB behavior (different from pandas)
    # DuckDB's concat_ws ignores NULL values and concatenates what's available
    expected_values = ["Title First Last", "Mr. John Doe", "Ms. Jane Smith", "Dr. Brown", "Prof. Alice"]
    assert lazy_result["formatted_name"].tolist() == expected_values
