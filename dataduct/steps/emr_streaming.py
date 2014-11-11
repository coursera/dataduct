"""
ETL step wrapper for EmrActivity can be executed on Ec2
"""
from .etl_step import ETLStep
from ..pipeline.emr_activity import EmrActivity
from ..s3.s3_file import S3File
from ..s3.s3_path import S3Path
from ..utils.exceptions import ETLInputError


def create_command(mapper, reducer, input_uri, output, hadoop_params):
    """Create the command step string given the input to streaming step
    """
    # TODO: Migrate to using Hadoop 2.x, allow param to determine what to use
    command = ['/home/hadoop/contrib/streaming/hadoop-streaming.jar']

    # Add hadoop parameters
    command.extend(hadoop_params)

    # Add mapper, output and reducer
    command.extend(['-mapper', mapper.s3_path.uri])
    command.extend(['-output', output.path().uri])
    if reducer:
        command.extend(['-reducer', reducer.s3_path.uri])

    # Add input uri
    if isinstance(input_uri, list):
        for i in input_uri:
            assert isinstance(i, S3Path)
            command.extend(['-input', i.uri])
    else:
        assert isinstance(input_uri, S3Path), type(input_uri)
        command.extend(['-input', input_uri.uri])

    return ','.join(command)


class EMRStreamingStep(ETLStep):
    """EMR Streaming Step class that helps run scripts on resouces
    """

    def __init__(self,
                 mapper,
                 reducer=None,
                 input=None,
                 hadoop_params=None,
                 depends_on=None,
                 **kwargs):
        """Constructor for the EMRStreamingStep class

        Args:
            mapper(path): local path to the mapper script
            reducer(path): local path to the reducer script
            input(str / list of str, optional): S3 Uri of input locations
            hadoop_params(list of str): arguments to the hadoop command
            **kwargs(optional): Keyword arguments directly passed to base class
        """

        # As EMR streaming allows inputs as both input_node and input
        # We remove the default input_node if input is given
        if input is not None:
            input_node = kwargs.pop('input_node', None)
        else:
            input_node = kwargs.get('input_node', None)

        if input is not None and 'input_node' in kwargs:
            raise ETLInputError('Both input and input_node specified')

        super(EMRStreamingStep, self).__init__(**kwargs)

        if depends_on is not None:
            self._depends_on = depends_on

        if hadoop_params is None:
            hadoop_params = []

        self._output = self.create_s3_data_node()

        # Create S3File with script / command provided
        mapper = self.create_script(S3File(path=mapper))
        additional_files = [mapper]

        if reducer is not None:
            reducer = self.create_script(S3File(path=reducer))
            additional_files.append(reducer)

        if input is not None:
            if isinstance(input, list):
                input = [S3Path(uri=i) for i in input]
            else:
                input = S3Path(uri=input)
        else:
            if isinstance(input_node, list):
                input = [i.path() for i in input_node]
            elif isinstance(input_node, dict):
                input = [i.path() for i in input_node.values()]
            else:
                input = input_node.path()

        step_string = create_command(mapper, reducer, input,
                                     self._output, hadoop_params)

        self.activity = self.create_pipeline_object(
            object_class=EmrActivity,
            resource=self.resource,
            schedule=self.schedule,
            emr_step_string=step_string,
            output_node=self._output,
            additional_files=additional_files,
            depends_on=self.depends_on,
            max_retries=self.max_retries
        )

    def merge_s3_nodes(self, input_nodes):
        """Override the merge S3Node case for EMR Streaming Step

        Args:
            input_nodes(dict): Map of the form {'node_name': node}

        Returns:
            output_node(list of S3Node): list of input nodes
            depends_on(list): Empty list
        """
        depends_on = []
        output_node = input_nodes.values()
        return output_node, depends_on
