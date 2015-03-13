"""Base class for QA steps that provides template function for publishing
"""
from boto.sns import SNSConnection
from datetime import datetime
import os

from .utils import render_output
from ..config import Config
from ..database import SelectStatement
from ..s3 import S3Path
from ..s3 import S3File
from ..utils import constants as const
from ..utils.helpers import exactly_one
from ..utils.helpers import get_s3_base_path

QA_TEST_ROW_LENGTH = 8


class Check(object):
    """Base class for QA steps that provides template function for publishing
    """
    def __init__(self, name, tolerance=0, sns_topic_arn=None):
        """Constructor for Check class

        Args:
            name(str): Name of the QA test
            tolerance(float): Error tolerance levels for the ETL
            sns_topic_arn(str): sns topic arn for QA test
        """
        self.name = name
        self.tolerance = tolerance
        if sns_topic_arn is None:
            config = Config()
            sns_topic_arn = config.etl.get('SNS_TOPIC_ARN_WARNING', None)
        self.sns_topic_arn = sns_topic_arn
        self.alert_func = self.get_sns_alert_function()

    def get_sns_alert_function(self):
        """Get a lamdda function for SNS alert publishing
        """
        if self.sns_topic_arn is None:
            return None
        return lambda message, subject: \
            SNSConnection().publish(self.sns_topic_arn, message, subject)

    @property
    def success(self):
        """True if error rate is below the tolerance levels
        """
        return self.error_rate is not None and \
            self.error_rate <= self.tolerance

    @property
    def summary(self):
        """Summary information about this test. This text must not
        contain any PII or otherwise sensitive data that cannot
        be published via email.
        """
        return render_output(
            [
                'Test Name: %s' % self.name,
                'Success: %s' % self.success
            ]
        )

    @property
    def results(self):
        """The results of this test. This may contain PII, as it
        should only be sent to S3 or Redshift. The default results are empty.
        Subclasses may override this.
        """
        # The default can just be the summary text as risk isn't increasing
        return self.summary

    @property
    def error_rate(self):
        """The error rate for the QA test
        """
        return None

    @property
    def export_output(self):
        """List of data associated with this check for analytics
        """
        return [
            self.name,
            1 if self.success else 0,
            self.tolerance,
            self.error_rate,
        ]

    @property
    def alert_subject(self):
        """String for alerts in case of calling the alert_func
        """
        return "Failure on %s" % self.name

    def publish(self, log_to_s3=False, dest_sql=None, table=None,
                path_suffix=None):
        """Publish the results of the QA test

        Note:
            Prints result summary, Exports check data, Call the alert function
            if specified
        """

        # Print results for logs
        print self.results
        print self.summary

        if log_to_s3:
            self.log_output_to_s3(dest_sql, table, path_suffix)

        if not self.success:
            if self.alert_func is not None:
                # Send summary to alert func for further publishing
                self.alert_func(self.summary, self.alert_subject)
            else:
                raise Exception(self.alert_subject)

    def log_output_to_s3(self, destination_sql=None, table=None,
                         path_suffix=None):
        """Log the results of the QA test in S3
        """
        if not exactly_one(destination_sql, table):
            raise Exception('Needs table or destination_sql')

        if destination_sql is not None:
            full_table_name = SelectStatement(destination_sql).dependencies[0]
        else:
            full_table_name = table

        config = Config()

        schema_name, table_name = full_table_name.split('.', 1)
        pipeline_name, _ = self.name.split(".", 1)
        timestamp = datetime.utcnow()

        row = [schema_name, table_name, pipeline_name, timestamp]
        row.extend(self.export_output)
        if len(row) < QA_TEST_ROW_LENGTH:
            row.extend(['NULL'] * (QA_TEST_ROW_LENGTH - len(row)))

        # Convert to TSV
        string = '\t'.join(map(str, row))

        # S3 Path computation
        qa_test_dir_uri = os.path.join(
            get_s3_base_path(), config.etl.get('QA_LOG_PATH', const.QA_STR),
            config.etl.get('DP_QA_TESTS_LOG_PATH', 'dba_table_qa_tests'),
            path_suffix if path_suffix else '')

        parent_dir = S3Path(uri=qa_test_dir_uri, is_directory=True)

        key = '_'.join(map(str, row)).replace('.', '_').replace(' ', '_')
        key += '.tsv'

        qa_tests_path = S3Path(key=key, parent_dir=parent_dir)
        qa_tests_file = S3File(text=string, s3_path=qa_tests_path)
        qa_tests_file.upload_to_s3()
