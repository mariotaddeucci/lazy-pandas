import duckdb
import pandas as pd
import pytest

from conftest import DataFramePair
from lazy_pandas import LazyFrame


@pytest.fixture
def large_df_pair():
    """Fixture that creates a larger DataFrame for sampling tests"""
    query = "SELECT * FROM range(100)"
    rel = duckdb.sql(query)
    # Create a LazyFrame and convert to pandas for the DataFramePair
    lazy_frame = LazyFrame(rel)
    return DataFramePair(pandas_df=lazy_frame.collect())


def test_sample_with_n(large_df_pair):
    """Tests the sample method with parameter n"""
    # Sampling with LazyFrame
    lazy_result = large_df_pair.lazy_df.sample(n=10).collect()

    # Verifications
    assert len(lazy_result) == 10

    # Reproducibility verification with random_state
    sample1 = large_df_pair.lazy_df.sample(n=5, random_state=42).collect()
    sample2 = large_df_pair.lazy_df.sample(n=5, random_state=42).collect()

    # Check if they are equal (same order and values)
    pd.testing.assert_frame_equal(sample1, sample2)


def test_sample_with_frac(large_df_pair):
    """Tests the sample method with parameter frac"""
    # Sampling with LazyFrame
    lazy_result = large_df_pair.lazy_df.sample(frac=0.1).collect()

    # Verification (we expect approximately 10 rows, but it can vary)
    assert 0 < len(lazy_result) < 30  # Allow some variation due to randomness

    # Check if different fractions result in different sizes
    small_sample = large_df_pair.lazy_df.sample(frac=0.05).collect()
    large_sample = large_df_pair.lazy_df.sample(frac=0.2).collect()

    # On average, the larger sample should have more rows than the smaller one
    # (there's a small chance this won't happen due to randomness)
    assert len(small_sample) <= len(large_sample)


def test_sample_error_cases(large_df_pair):
    """Tests error cases for the sample method"""
    # Conditional tests to not break if the API is different

    # Neither n nor frac specified
    with pytest.raises((ValueError, TypeError)):
        large_df_pair.lazy_df.sample().collect()

    # Both n and frac specified
    with pytest.raises((ValueError, TypeError)):
        large_df_pair.lazy_df.sample(n=10, frac=0.1).collect()

    # frac = 0 (must be > 0)
    with pytest.raises((ValueError, TypeError)):
        large_df_pair.lazy_df.sample(frac=0).collect()

    with pytest.raises((ValueError, TypeError)):
        large_df_pair.lazy_df.sample(frac=1.5).collect()
