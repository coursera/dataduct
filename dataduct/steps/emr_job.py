"""
ETL step wrapper for EmrActivity can be executed on EMR Cluster
"""
from .etl_step import ETLStep
from ..pipeline import EmrActivity
from ..utils import constants as const


class EMRJobStep(ETLStep):
    """EMR Step class that helps run a step on the emr cluster
    """

    def __init__(self,
                 step_string,
                 **kwargs):
        """Constructor for the EMRJobStep class

        Args:
            step_string(str): Step string for the emr job to be executed
            **kwargs(optional): Keyword arguments directly passed to base class

        Note:
            In the step_string all comma within arguments should be escaped
            using 4 backslashes
        """
        super(EMRJobStep, self).__init__(**kwargs)

        self.activity = self.create_pipeline_object(
            object_class=EmrActivity,
            resource=self.resource,
            worker_group=self.worker_group,
            input_node=self.input,
            schedule=self.schedule,
            emr_step_string=step_string,
            output_node=self.output,
            depends_on=self.depends_on,
            max_retries=self.max_retries
        )

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        step_args = cls.base_arguments_processor(
            etl, input_args, resource_type=const.EMR_CLUSTER_STR)

        return step_args
