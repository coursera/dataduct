"""
ETL step for pipeline dependencies using transform step
"""
from ..config import Config
from ..utils import constants as const
from .transform import TransformStep

config = Config()
NAME_PREFIX = config.etl.get('NAME_PREFIX', '')
DEPENDENCY_OVERRIDE = config.etl.get('DEPENDENCY_OVERRIDE', False)
SNS_TOPIC_ARN = config.etl.get('SNS_TOPIC_ARN_FAILURE', None)


class PipelineDependenciesStep(TransformStep):
    """PipelineDependencies Step class that helps wait for other pipelines
        to finish
    """

    def __init__(self,
                 id,
                 pipeline_name,
                 dependent_pipelines=None,
                 dependent_pipelines_ok_to_fail=None,
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

        if (dependent_pipelines is None and
                dependent_pipelines_ok_to_fail is None):
            raise ValueError('Must have some dependencies for dependency step')

        prefix_func = lambda p: p if not NAME_PREFIX else NAME_PREFIX + '_' + p
        argument_func = lambda x: [prefix_func(p) for p in x]

        if DEPENDENCY_OVERRIDE:
            command = 'ls'
            script_arguments = None
        else:
            command = const.DEPENDENCY_COMMAND
            if start_date is None:
                start_date = "#{format(@scheduledStartTime,'YYYY-MM-dd')}"

            script_arguments.extend(
                [
                    '--pipeline_name=%s' % pipeline_name,
                    '--start_date=%s' % start_date,
                    '--refresh_rate=%s' % str(refresh_rate),
                    '--sns_topic_arn=%s' % SNS_TOPIC_ARN,
                ]
            )

            if dependent_pipelines:
                script_arguments.append('--dependencies')
                script_arguments.extend(argument_func(dependent_pipelines))

            if dependent_pipelines_ok_to_fail:
                script_arguments.append('--dependencies_ok_to_fail')
                script_arguments.extend(
                    argument_func(dependent_pipelines_ok_to_fail))

        super(PipelineDependenciesStep, self).__init__(
            id=id,
            command=command,
            script_arguments=script_arguments,
            no_output=True,
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
        step_args['pipeline_name'] = etl.name

        return step_args
