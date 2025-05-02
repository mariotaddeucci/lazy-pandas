import pytest
from lazy_pandas.exceptions import ContributionAcceptedError
from conftest import skip_on_contribution_error


def test_contribution_error_basic():
    """Test that ContributionAcceptedError can be raised and caught normally"""
    try:
        raise ContributionAcceptedError("This feature needs contribution")
        pytest.fail("Exception was not raised")
    except ContributionAcceptedError as e:
        assert "needs contribution" in str(e)
        assert isinstance(e, NotImplementedError)


def test_contribution_error_inheritance():
    """Test that ContributionAcceptedError inherits correctly from NotImplementedError"""
    err = ContributionAcceptedError("Test message")
    assert isinstance(err, NotImplementedError)
    assert isinstance(err, Exception)


def function_raising_contribution_error():
    """Helper function that raises ContributionAcceptedError"""
    raise ContributionAcceptedError("This feature is not implemented yet but contributions are welcome")


@skip_on_contribution_error
def test_contribution_error_auto_skip():
    """This test should be automatically skipped due to ContributionAcceptedError"""
    # The decorator should catch this and skip the test
    function_raising_contribution_error()
    # If not skipped, this would fail
    pytest.fail("Test was not skipped as expected")


def function_raising_regular_error():
    """Helper function that raises a regular Exception"""
    raise ValueError("Regular error")


def test_regular_exception_not_skipped():
    """
    This test should fail with a regular exception.
    We're using pytest.raises to catch it, but in a normal test it would cause a failure.
    """
    with pytest.raises(ValueError):
        function_raising_regular_error()


def test_unsupported_operation_error():
    """Test that ContributionAcceptedError is raised for unsupported operations"""
    with pytest.raises(ContributionAcceptedError):
        raise ContributionAcceptedError("This feature is not supported")
