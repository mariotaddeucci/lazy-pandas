from lazy_pandas.exceptions import ContributionAcceptedError


class FeatureExample:
    """Example class demonstrating the use of ContributionAcceptedError"""

    def implemented_feature(self):
        """This feature is fully implemented"""
        return "Feature works"

    def not_implemented_feature(self):
        """
        This feature is not implemented yet, but contributions are welcome.

        When tested, this will automatically be skipped rather than failing.
        """
        raise ContributionAcceptedError(
            "This feature is not implemented yet, but we welcome contributions! "
            "Please consider submitting a pull request."
        )
