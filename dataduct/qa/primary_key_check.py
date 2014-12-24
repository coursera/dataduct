"""QA test for we have duplicate primary keys inside redshift
"""

from .check import Check
from .utils import render_output


class PrimaryKeyCheck(Check):
    """QA test for checking duplicate primary keys inside redshift
    """
    def __init__(self, duplicate_count=0, **kwargs):
        """Constructor for Primary Key Check

        Args:
            duplicate_count(int): Number of duplicates
        """
        super(PrimaryKeyCheck, self).__init__(**kwargs)
        self.duplicate_count = duplicate_count

    @property
    def error_rate(self):
        """The error rate for the QA test
        """
        return self.duplicate_count

    @property
    def summary(self):
        """Summary of the test results for the SNS message
        """
        return render_output(
            [
                'Test Name: %s' % self.name,
                'Success: %s' % self.success,
                'Tolerance: %d' % self.tolerance,
                'Error Rate: %d' % self.error_rate,
            ]
        )
