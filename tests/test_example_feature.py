from example_feature import FeatureExample
from conftest import skip_on_contribution_error


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
