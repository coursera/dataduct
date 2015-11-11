"""ETL step wrapper for Reload SQL script
"""
from .upsert import UpsertStep


class ReloadStep(UpsertStep):
    """Reload Step class that helps run a step on the emr cluster
    """

    def __init__(self, **kwargs):
        """Constructor for the ReloadStep class

        Args:
            **kwargs(optional): Keyword arguments directly passed to base class
        """

        # Enforce PK by default.
        if 'enforce_primary_key' not in kwargs:
            kwargs['enforce_primary_key'] = True
        super(ReloadStep, self).__init__(**kwargs)

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        input_args['delete_existing'] = True
        return super(ReloadStep, cls).arguments_processor(etl, input_args)
