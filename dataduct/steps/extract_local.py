"""
ETL step wrapper for creating an S3 node for input from local files
"""
from ..s3 import S3File
from ..utils.exceptions import ETLInputError
from .etl_step import ETLStep


class ExtractLocalStep(ETLStep):
    """ExtractLocal Step class that helps get data from a local file
    """

    def __init__(self, path, output_path=None, **kwargs):
        """Constructor for the ExtractLocalStep class

        Args:
            path(str): local path for data
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        super(ExtractLocalStep, self).__init__(**kwargs)
        self._output = self.create_s3_data_node(
            S3File(path=path, s3_path=self.get_output_s3_path(output_path)))

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        input_args = cls.pop_inputs(input_args)
        step_args = cls.base_arguments_processor(etl, input_args)

        step_args.pop('resource', None)
        step_args.pop('worker_group', None)
        if etl.frequency != 'one-time':
            raise ETLInputError(
                'Extract Local can be used for one-time pipelines only')

        return step_args
