"""
Example test demonstrating the use of ContributionAcceptedError.

Note: This is a demonstration file. In actual use, the skip_on_contribution_error
decorator is available from tests/conftest.py when running tests in the main test suite.

For this example to work standalone, you would need to either:
1. Copy the decorator definition here, or
2. Run it as part of the test suite where conftest.py is available
"""

import pytest
from example_feature import FeatureExample

# Note: In the actual test suite, this would be imported from conftest
# For standalone use, the decorator is defined inline here
def skip_on_contribution_error(func):
    """
    Decorator that automatically skips a test if ContributionAcceptedError is raised.
    """
    from lazy_pandas.exceptions import ContributionAcceptedError
    
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ContributionAcceptedError as e:
            pytest.skip(f"Skipped due to ContributionAcceptedError: {str(e)}")
    
    return wrapper


def test_implemented_feature():
    """Test that a normal implemented feature works correctly"""
    feature = FeatureExample()
    result = feature.implemented_feature()
    assert result == "Feature works"


@skip_on_contribution_error
def test_not_implemented_feature():
    """
    Test for a feature that's not implemented yet.

    This test should be automatically skipped due to the ContributionAcceptedError
    being raised by the implementation.
    """
    feature = FeatureExample()
    # This will raise ContributionAcceptedError, and the decorator will catch it
    # and mark the test as skipped
    result = feature.not_implemented_feature()
    # The following assertion should never execute because the test is skipped
    assert result is not None
