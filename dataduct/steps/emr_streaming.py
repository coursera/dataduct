"""
ETL step wrapper for EmrStreamingActivity can be executed on EMR Cluster
"""
from .etl_step import ETLStep
from ..pipeline import EmrActivity
from ..s3 import S3File
from ..utils import constants as const

HADOOP_1_SERIES = ['1', '2']


def create_command_hadoop_1(mapper, reducer, command, command_options):
    """Create the command step string for Hadoop 1.x

    Note:
        -mapper,s3://dataduct/word_mapper.py,
        -reducer,s3://dataduct/word_reducer.py
    """
    command_options.extend(['-mapper', mapper.s3_path.uri])
    if reducer:
        command_options.extend(['-reducer', reducer.s3_path.uri])

    command.extend(command_options)
    return ','.join(command)


def create_command_hadoop_2(mapper, reducer, command, command_options):
    """Create the command step string for Hadoop 2.x

    Note:
        -files,s3://dataduct/word_mapper.py\\,s3://dataduct/word_reducer.py,
        -mapper,word_mapper.py,
        -reducer,word_reducer.py
    """
    files = [mapper.s3_path.uri]
    command_options.extend(['-mapper', mapper.s3_path.base_filename])
    if reducer:
        files.append(reducer.s3_path.uri)
        command_options.extend(['-reducer', reducer.s3_path.base_filename])

    # Note: We need to add generic options like files before command options
    # Comma's need to be escaped
    command.extend(['-files', '\\\\,'.join(files)])
    command.extend(command_options)

    return ','.join(command)


def create_command(mapper, reducer, ami_version, input, output,
                   hadoop_params):
    """Create the command step string given the input to streaming step
    """
    ami_family = ami_version.split('.')[0]

    command = ['/home/hadoop/contrib/streaming/hadoop-streaming.jar']
    command_options = []

    # Add hadoop parameters
    # Note: We need to add generic options like files before command options
    if hadoop_params is not None:
        command.extend(hadoop_params)

    # Add output uri
    command_options.extend(['-output', output.path().uri])

    # Add input uri
    command_options.extend(['-input', input.path().uri])

    if ami_family in HADOOP_1_SERIES:
        return create_command_hadoop_1(mapper, reducer, command,
                                       command_options)

    return create_command_hadoop_2(mapper, reducer, command,
                                   command_options)


class EMRStreamingStep(ETLStep):
    """EMR Streaming Step class that helps run scripts on resouces
    """

    def __init__(self,
                 mapper,
                 reducer=None,
                 hadoop_params=None,
                 output_path=None,
                 **kwargs):
        """Constructor for the EMRStreamingStep class

        Args:
            mapper(path): local path to the mapper script
            reducer(path): local path to the reducer script
            input(str / list of str, optional): S3 Uri of input locations
            hadoop_params(list of str): arguments to the hadoop command
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        super(EMRStreamingStep, self).__init__(**kwargs)

        self._output = self.create_s3_data_node(
            self.get_output_s3_path(output_path))

        # Create S3File with script / command provided
        mapper = self.create_script(S3File(path=mapper))
        additional_files = [mapper]

        if reducer is not None:
            reducer = self.create_script(S3File(path=reducer))
            additional_files.append(reducer)

        step_string = create_command(mapper, reducer, self.resource.ami_version,
                                     self.input, self.output, hadoop_params)

        self.activity = self.create_pipeline_object(
            object_class=EmrActivity,
            resource=self.resource,
            worker_group=self.worker_group,
            input_node=self.input,
            schedule=self.schedule,
            emr_step_string=step_string,
            output_node=self.output,
            additional_files=additional_files,
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
