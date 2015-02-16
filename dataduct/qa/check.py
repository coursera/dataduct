"""Base class for QA steps that provides template function for publishing
"""
from boto.sns import SNSConnection
from ..config import Config
from .utils import render_output


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

    def publish(self, export_func=None):
        """Publish the results of the QA test

        Note:
            Prints result summary, Exports check data, Call the alert function
            if specified
        """

        # Print results for logs
        print self.results
        print self.summary

        if export_func is not None:
            export_func(self.export_output)

        if not self.success:
            if self.alert_func is not None:
                # Send summary to alert func for further publishing
                self.alert_func(self.summary, self.alert_subject)
            else:
                raise Exception(self.alert_subject)
