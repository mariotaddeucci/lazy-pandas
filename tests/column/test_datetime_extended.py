import pandas as pd
import pytest

from conftest import DataFramePair


@pytest.fixture
def datetime_extended_df():
    """Fixture that creates a DataFrame with a variety of dates for more comprehensive tests"""
    return DataFramePair(
        query="""
        SELECT cast('2023-01-01' as datetime) AS dt_time
        UNION ALL SELECT cast('2023-12-31 00:00:00' as datetime)  -- year end without time part
        UNION ALL SELECT cast('2024-02-29 12:30:45' as datetime)  -- leap year
        UNION ALL SELECT cast('2023-03-31 00:00:00' as datetime)  -- month end
        UNION ALL SELECT NULL                                    -- null value
    """
    )


def test_dt_date_parts(datetime_extended_df):
    """Tests various date/time components simultaneously"""
    # Apply various date component extraction operations
    df = datetime_extended_df.lazy_df

    # Year
    df["year"] = df["dt_time"].dt.year
    # Month
    df["month"] = df["dt_time"].dt.month
    # Day
    df["day"] = df["dt_time"].dt.day
    # Hour
    df["hour"] = df["dt_time"].dt.hour

    # Collect the result
    result = df.collect()

    # Specific value checks for each row
    # First row: 2023-01-01 00:00:00
    assert result.iloc[0]["year"] == 2023
    assert result.iloc[0]["month"] == 1
    assert result.iloc[0]["day"] == 1
    assert result.iloc[0]["hour"] == 0

    # Second row: 2023-12-31 00:00:00
    assert result.iloc[1]["year"] == 2023
    assert result.iloc[1]["month"] == 12
    assert result.iloc[1]["day"] == 31
    assert result.iloc[1]["hour"] == 0

    # Third row: 2024-02-29 12:30:45 (leap year)
    assert result.iloc[2]["year"] == 2024
    assert result.iloc[2]["month"] == 2
    assert result.iloc[2]["day"] == 29
    assert result.iloc[2]["hour"] == 12


def test_dt_is_special_days(datetime_extended_df):
    """Tests methods for verifying special days (beginning/end of period)"""
    df = datetime_extended_df.lazy_df

    # Checking special days
    df["is_month_start"] = df["dt_time"].dt.is_month_start
    df["is_month_end"] = df["dt_time"].dt.is_month_end
    df["is_quarter_start"] = df["dt_time"].dt.is_quarter_start
    df["is_year_start"] = df["dt_time"].dt.is_year_start
    df["is_year_end"] = df["dt_time"].dt.is_year_end

    # Collect the result
    result = df.collect()

    # Checks for first day of month (01/01/2023)
    assert result.iloc[0]["is_month_start"]
    assert result.iloc[0]["is_quarter_start"]
    assert result.iloc[0]["is_year_start"]

    # Checks for last day of month/year (31/12/2023)
    # Now without the time part (00:00:00), should work correctly
    assert result.iloc[1]["is_month_end"]
    assert result.iloc[1]["is_year_end"]

    # Checks for last day of month (31/03/2023)
    assert result.iloc[3]["is_month_end"]

    # Check with null values
    assert pd.isna(result.iloc[4]["is_month_start"])


def test_dt_weekday(datetime_extended_df):
    """Tests weekday method"""
    df = datetime_extended_df.lazy_df

    # Extracting weekday
    df["weekday"] = df["dt_time"].dt.weekday()

    # Collect the result
    result = df.collect()

    # In the current implementation, weekday is 0-indexed
    # (0 = Sunday, 1 = Monday, ..., 6 = Saturday), as in standard pandas
    # Checks for 01/01/2023 (should be Sunday, day 0)
    assert result.iloc[0]["weekday"] == 0  # Sunday = 0

    # Checks for 31/12/2023 (should be Sunday)
    assert result.iloc[1]["weekday"] == 0  # Sunday = 0

    # Checks for 29/02/2024 (should be Thursday)
    assert result.iloc[2]["weekday"] == 4  # Thursday = 4
