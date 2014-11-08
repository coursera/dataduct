"""
ETL step wrapper for shell command activity can be executed on Ec2 / EMR
"""
from .etl_step import ETLStep
from ..pipeline.shell_command_activity import ShellCommandActivity
from ..s3.s3_file import S3File
from ..utils.helpers import exactly_one
from ..utils.exceptions import ETLInputError


class TransformStep(ETLStep):
    """Transform Step class that helps run scripts on resouces
    """

    def __init__(self,
                 command=None,
                 script=None,
                 output=None,
                 script_arguments=None,
                 additional_s3_files=None,
                 **kwargs):
        """Constructor for the TransformStep class

        Args:
            command(str): s3 directory for pipeline logs
            script(path): pipeline schedule used for the machine
            output(dict): time to terminate the ec2resource after
            script_arguments(str): machine type to be used eg. m1.large
            additional_s3_files(str): time delay between step retries
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        if not exactly_one(command, script):
            raise ETLInputError('Both command or script found')

        super(TransformStep, self).__init__(**kwargs)

        # Create output_node if not provided
        if self._output is None:
            output_node = self.create_s3_data_node()
        else:
            output_node = self._output

        # Create S3File if script path provided
        if script:
            script = self.create_script(S3File(path=script))

        self.create_pipeline_object(
            object_class=ShellCommandActivity,
            input_node=self._input_node,
            output_node=output_node,
            resource=self.resource,
            schedule=self.schedule,
            script_uri=script,
            script_arguments=script_arguments,
            command=command,
            max_retries=self.max_retries,
            depends_on=self.depends_on,
            additional_s3_files=additional_s3_files,
        )

        # Translate output nodes if output map provided
        if self._output is None:
            if output:
                self._output = self.create_output_nodes(output_node, output)
            else:
                self._output = output_node
