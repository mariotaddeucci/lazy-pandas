import pytest

from conftest import DataFramePair


@pytest.fixture
def string_column_df():
    """Fixture that creates a DataFrame with text columns for string operations testing"""
    return DataFramePair(
        query="""
        SELECT 'Hello' AS text, '  abc  ' AS padded, 'xyz123' AS mixed, NULL AS empty
        UNION ALL SELECT 'World', 'def', '456', 'not empty'
        UNION ALL SELECT 'Test', '', 'FOO', NULL
        """
    )


def test_string_find_method(string_column_df):
    """Tests the find method for searching substrings"""
    # Applying find in LazyFrame
    string_column_df.lazy_df["pos_l"] = string_column_df.lazy_df["text"].str.find("l")
    string_column_df.lazy_df["pos_o"] = string_column_df.lazy_df["text"].str.find("o")
    string_column_df.lazy_df["pos_not"] = string_column_df.lazy_df["text"].str.find("xyz")

    result = string_column_df.lazy_df.collect()

    # Verifications (adjusted for the actual implementation behavior)
    # 'l' is at position 2 in "Hello", position 3 in "World" (not -1), doesn't exist in "Test"
    assert result["pos_l"].tolist() == [2, 3, -1]
    assert result["pos_o"].tolist() == [4, 1, -1]  # "o" is at position 4 in "Hello", 1 in "World", not in "Test"
    assert result["pos_not"].tolist() == [-1, -1, -1]  # "xyz" doesn't exist in any string


def test_string_pad_methods(string_column_df):
    """Tests the pad, ljust, rjust and zfill methods"""
    # Applying pad in LazyFrame
    string_column_df.lazy_df["lpad"] = string_column_df.lazy_df["text"].str.pad(10, side="left", fillchar="*")
    string_column_df.lazy_df["rpad"] = string_column_df.lazy_df["text"].str.pad(10, side="right", fillchar="#")

    # Using ljust and rjust - correctly matching their intended behavior
    string_column_df.lazy_df["ljust"] = string_column_df.lazy_df["text"].str.ljust(10, "-")
    string_column_df.lazy_df["rjust"] = string_column_df.lazy_df["text"].str.rjust(10, "+")

    # Using zfill
    string_column_df.lazy_df["zfill"] = string_column_df.lazy_df["text"].str.zfill(10)

    result = string_column_df.lazy_df.collect()

    # Verifications for pad
    assert result["lpad"].tolist() == ["*****Hello", "*****World", "******Test"]
    assert result["rpad"].tolist() == ["Hello#####", "World#####", "Test######"]

    # Verifications for ljust and rjust (now fixed to match correct behavior)
    # ljust adds characters to the right (left-justified text)
    assert result["ljust"].tolist() == ["Hello-----", "World-----", "Test------"]
    # rjust adds characters to the left (right-justified text)
    assert result["rjust"].tolist() == ["+++++Hello", "+++++World", "++++++Test"]

    # Verifications for zfill
    assert result["zfill"].tolist() == ["00000Hello", "00000World", "000000Test"]

    # Test for side="both" (not implemented)
    with pytest.raises(NotImplementedError):
        string_column_df.lazy_df["text"].str.pad(10, side="both")

    # Test for invalid side
    with pytest.raises(ValueError):
        string_column_df.lazy_df["text"].str.pad(10, side="invalid")


def test_string_advanced_methods(string_column_df):
    """Tests advanced string methods that aren't well covered"""
    # Testing contains, startswith and endswith
    string_column_df.lazy_df["contains_e"] = string_column_df.lazy_df["text"].str.contains("e")
    string_column_df.lazy_df["starts_t"] = string_column_df.lazy_df["text"].str.startswith("T")
    string_column_df.lazy_df["ends_d"] = string_column_df.lazy_df["text"].str.endswith("d")

    result = string_column_df.lazy_df.collect()

    # Verifications
    assert result["contains_e"].tolist() == [True, False, True]
    assert result["starts_t"].tolist() == [False, False, True]
    assert result["ends_d"].tolist() == [False, True, False]


def test_string_new_methods(string_column_df):
    """Tests the newly added string methods: repeat, capitalize, slice, and is* methods"""
    # Testing repeat
    string_column_df.lazy_df["repeat"] = string_column_df.lazy_df["text"].str.repeat(2)

    # Testing capitalize
    string_column_df.lazy_df["capitalized"] = string_column_df.lazy_df["text"].str.capitalize()

    # Testing slice with different parameters
    string_column_df.lazy_df["slice_1_3"] = string_column_df.lazy_df["text"].str.slice(1, 3)
    string_column_df.lazy_df["slice_start"] = string_column_df.lazy_df["text"].str.slice(2)

    # Testing isalpha, isalnum, isdigit, isnumeric on mixed column
    string_column_df.lazy_df["is_alpha"] = string_column_df.lazy_df["mixed"].str.isalpha()
    string_column_df.lazy_df["is_alnum"] = string_column_df.lazy_df["mixed"].str.isalnum()
    string_column_df.lazy_df["is_digit"] = string_column_df.lazy_df["mixed"].str.isdigit()
    string_column_df.lazy_df["is_numeric"] = string_column_df.lazy_df["mixed"].str.isnumeric()

    result = string_column_df.lazy_df.collect()

    # Verifications for repeat
    assert result["repeat"].tolist() == ["HelloHello", "WorldWorld", "TestTest"]

    # Verifications for capitalize
    # Note: Our implementation converts to lowercase after capitalizing the first letter
    assert result["capitalized"].tolist() == ["Hello", "World", "Test"]

    # Verifications for slice
    assert result["slice_1_3"].tolist() == ["el", "or", "es"]
    assert result["slice_start"].tolist() == ["llo", "rld", "st"]

    # Verifications for is* methods
    # mixed column values: ['xyz123', '456', 'FOO']
    assert result["is_alpha"].tolist() == [False, False, True]  # Only 'FOO' is all alphabetic
    assert result["is_alnum"].tolist() == [True, True, True]  # All are alphanumeric
    assert result["is_digit"].tolist() == [False, True, False]  # Only '456' is all digits
    assert result["is_numeric"].tolist() == [False, True, False]  # Same as isdigit in our implementation
