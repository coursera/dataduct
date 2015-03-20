"""QA test for comparing columns in the source system with the Warehouse
"""
from .check import Check
from .utils import render_output


class ColumnCheck(Check):
    """QA test for comparing columns across the ETL
    """
    def __init__(self, source_data, destination_data, **kwargs):
        """Constructor for the Count based QA

        Args:
            source_data(DataFrame): Sample of source data
            destination_data(DataFrame): Sample of destination data
        """
        super(ColumnCheck, self).__init__(**kwargs)
        self.source_data = source_data
        self.destination_data = destination_data
        self.errors = []
        self.observed = 0

        # Identify errors
        for key in source_data.index:
            if key not in destination_data.index:
                continue

            source_value = ColumnCheck.column_value(self.source_data, key)
            dest_value = ColumnCheck.column_value(self.destination_data, key)

            if source_value != dest_value:
                self.errors.append((key, source_value, dest_value))
            self.observed += 1

    @property
    def error_rate(self):
        """The error rate for the column comparisons

        Note:
            The error is only calculated for keys that exist in both dataframes.
            Thus, we presume that issues dealing with row counts are addressed
            in a separate QA test.
        """
        if self.observed == 0:
            return None

        return float(len(self.errors) * 100) / self.observed

    @staticmethod
    def column_value(data, key):
        """Fetch the value for a key in the dataframe

        Args:
            data(DataFrame): Single column dataframe
            key(str): Key to lookup in the dataframe

        Returns:
            value(str): Value for the key, unicode values are encoded as utf-8
        """
        value = data.loc[key].values[0]
        if isinstance(value, unicode):
            return value.encode('utf-8')
        return value

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
                'Observed: %d' % self.observed,
            ]
        )

    @property
    def results(self):
        """Results from the the comparison of the errors
        """
        return render_output([str(a) for a in self.errors])
