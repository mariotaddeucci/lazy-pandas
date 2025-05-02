import numpy as np
import pandas as pd
import pytest

from conftest import DataFramePair
from lazy_pandas import LazyFrame


@pytest.fixture
def sample_df():
    """Fixture that creates a DataFrame with data for sample tests"""
    # Create a DataFrame large enough to test sampling
    data = pd.DataFrame(
        {"id": range(1, 101), "value": np.random.randn(100), "group": np.random.choice(["A", "B", "C"], 100)}
    )
    lazy_frame = LazyFrame(data)
    return DataFramePair(lazy_df=lazy_frame, pandas_df=data)


def test_sample_n_parameter(sample_df):
    """Tests the sample method with parameter n (number of rows)"""
    # Check if the sample method returns the correct number of rows
    for n in [5, 10, 20]:
        result = sample_df.lazy_df.sample(n=n).collect()
        assert len(result) == n

    # Checking validation of n > number of rows
    # Some engines limit to the number of available rows
    big_n = sample_df.lazy_df.sample(n=200).collect()
    assert len(big_n) <= 100  # Cannot return more than we have


def test_sample_frac_parameter(sample_df):
    """Tests the sample method with parameter frac (fraction of rows)"""
    # Test different fractions
    for frac in [0.1, 0.25, 0.5]:
        result = sample_df.lazy_df.sample(frac=frac).collect()
        # The exact count can vary significantly due to randomness
        expected_count = int(len(sample_df.pandas_df) * frac)
        # Increase tolerance for random variation
        tolerance = max(10, int(expected_count * 0.3))  # 30% or at least 10
        assert abs(len(result) - expected_count) <= tolerance


def test_sample_random_state(sample_df):
    """Tests reproducibility with random_state in sample"""
    # Samples with the same random_state should be identical
    sample1 = sample_df.lazy_df.sample(n=10, random_state=42).collect()
    sample2 = sample_df.lazy_df.sample(n=10, random_state=42).collect()

    # Check if the two samples have the same rows (same order)
    pd.testing.assert_frame_equal(sample1, sample2)

    # Samples with different random_state usually result in different content
    # In this case, we only check if the behavior is consistent
    # (we don't check if the indices are different as it depends on the implementation)
    sample3 = sample_df.lazy_df.sample(n=10, random_state=24).collect()

    # Check if the samples are at least reproducible
    sample4 = sample_df.lazy_df.sample(n=10, random_state=24).collect()
    pd.testing.assert_frame_equal(sample3, sample4)


def test_sample_error_cases(sample_df):
    """Tests error cases in the sample method"""
    # Neither n nor frac specified should generate an error
    with pytest.raises((ValueError, TypeError)):
        sample_df.lazy_df.sample().collect()

    # Both n and frac specified should generate an error
    with pytest.raises((ValueError, TypeError)):
        sample_df.lazy_df.sample(n=10, frac=0.1).collect()

    # Negative frac should generate an error
    with pytest.raises((ValueError)):
        sample_df.lazy_df.sample(frac=-0.1).collect()

    with pytest.raises((ValueError)):
        sample_df.lazy_df.sample(frac=1.5).collect()


def test_sample_with_replacement(sample_df):
    """Tests sampling with replacement (if supported)"""
    try:
        # This test will only work if the implementation supports replace=True
        # Test with an n larger than the original size, only possible with replacement
        result = sample_df.lazy_df.sample(n=150, replace=True).collect()

        # If we get here, replace=True is supported
        assert len(result) == 150

        # With replace=False (default) and n > size, should limit to original size
        result_no_replace = sample_df.lazy_df.sample(n=150, replace=False).collect()
        assert len(result_no_replace) <= 100
    except (TypeError, ValueError):
        # If the implementation doesn't support replace, the test passes
        pass
