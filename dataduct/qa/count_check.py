"""QA test for comparing number of rows in the source system with the Warehouse
"""

from .check import Check
from .utils import render_output


class CountCheck(Check):
    """QA test for comparing number of rows across the ETL
    """
    def __init__(self, source_count, destination_count, **kwargs):
        """Constructor for the Count based QA

        Args:
            source_count(int): Count of objects in the source system
            destination_count(int): Count of objects in the warehouse
        """
        super(CountCheck, self).__init__(**kwargs)
        self.source_count = source_count
        self.destination_count = destination_count

    @property
    def error_rate(self):
        """The error rate.
        If there are no values in the source or destination, the error is 0.
        If there are no values in the source but some in the destination,
        the error is None
        """
        return self.calculate_error_rate(self.source_count,
                                         self.destination_count)

    @staticmethod
    def calculate_error_rate(source_count, destination_count):
        """Calculate the error rate based on the source and destination counts
        """
        if source_count > 0:
            error_difference = float(source_count - destination_count)
            return abs(error_difference * 100) / source_count
        elif destination_count == 0:
            return 0
        else:
            return None

    @property
    def summary(self):
        """Summary of the test results for the SNS message
        """
        return render_output(
            [
                'Test Name: %s' % self.name,
                'Success: %s' % self.success,
                'Tolerance: %0.4f%%' % self.tolerance,
                'Error Rate: %0.4f%%' % self.error_rate,
                'Source Count: %d' % self.source_count,
                'Destination Count: %d' % self.destination_count,
            ]
        )
