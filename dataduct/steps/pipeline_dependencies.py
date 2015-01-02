"""
ETL step for pipeline dependencies using transform step
"""
import os

from .transform import TransformStep
from ..utils import constants as const


class PipelineDependenciesStep(TransformStep):
    """PipelineDependencies Step class that helps wait for other pipelines
        to finish
    """

    def __init__(self,
                 id,
                 dependent_pipelines=None,
                 refresh_rate=300,
                 start_date=None,
                 script_arguments=None,
                 **kwargs):
        """Constructor for the QATransformStep class

        Args:
            sns_arn(str): sns topic arn for QA steps
            script_arguments(list of str): list of arguments to the script
            **kwargs(optional): Keyword arguments directly passed to base class
        """

        if script_arguments is None:
            script_arguments = list()

        if dependent_pipelines is None:
            raise ValueError('Must have some dependencies for dependency step')

        if start_date is None:
            start_date = "#{format(@scheduledStartTime,'YYYY-MM-dd')}"

        script_arguments.extend(
            [
                '--start_date=%s' % start_date,
                '--refresh_rate=%s' % str(refresh_rate),
                '--dependencies',
            ]
        )
        script_arguments.extend(dependent_pipelines)

        steps_path = os.path.abspath(os.path.dirname(__file__))
        script = os.path.join(steps_path, const.DEPENDENCY_SCRIPT_PATH)

        super(PipelineDependenciesStep, self).__init__(
            id=id,
            script=script,
            script_arguments=script_arguments,
            **kwargs)

        self._output = None

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        input_args = cls.pop_inputs(input_args)
        step_args = cls.base_arguments_processor(etl, input_args)
        step_args['resource'] = etl.ec2_resource

        return step_args
