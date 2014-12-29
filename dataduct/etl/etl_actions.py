"""
Script that parses the pipeline definition and has action functions
"""
import yaml

from ..pipeline import Activity
from ..pipeline import MysqlNode
from ..pipeline import RedshiftNode
from ..pipeline import S3Node
from .etl_pipeline import ETLPipeline
from ..utils.exceptions import ETLInputError

URL_TEMPLATE = 'https://console.aws.amazon.com/datapipeline/?#ExecutionDetailsPlace:pipelineId={ID}&show=latest'  # noqa


def read_pipeline_definition(file_path):
    """Function reads the yaml pipeline definitions.

    Function reads the yaml pipeline definitions. We also remove the variables
    key as that was only used for yaml placeholders.

    Args:
      file_path (str): Path to the pipeline definition.

    Returns:
      dict: parsed yaml definition as dictionary.

    Raises:
      ETLInputError: If `file_path` extention is not yaml
    """
    extention = file_path.split('.').pop()
    if extention != 'yaml':
        raise ETLInputError('Pipeline definition should have a yaml extention')
    with open(file_path) as f:
        definition = yaml.load(f.read())

        # remove the variables key from the pipeline definition
        # http://stackoverflow.com/questions/4150782/using-yaml-with-variables
        definition.pop('variables', None)
        definition.pop('description', None)

    return definition


def create_pipeline(definition):
    """Creates the pipeline and add the steps specified to the pipeline

    Args:
        definition(dict): YAML definition parsed from the datapipeline
    """
    steps = definition.pop('steps')
    etl = ETLPipeline(**definition)

    # Add the steps to the pipeline object
    etl.create_steps(steps)
    print 'Created pipeline. Name: %s' % etl.name

    return etl


def validate_pipeline(etl, force_overwrite=False):
    """Validates the pipeline that was created

    Args:
        etl(EtlPipeline): pipeline object that needs to be validated
        force_overwrite(bool): delete if a pipeline of same name exists
    """
    if force_overwrite:
        etl.delete_if_exists()
    etl.validate()
    print 'Validated pipeline. Id: %s' % etl.pipeline.id


def activate_pipeline(etl):
    """Activate the pipeline that was created

    Args:
        etl(EtlPipeline): pipeline object that needs to be activated
    """
    etl.activate()
    print 'Activated pipeline. Id: %s' % etl.pipeline.id
    print 'Monitor pipeline here: %s' % \
        URL_TEMPLATE.format(ID=etl.pipeline.id)


def visualize_pipeline(etl, filename=None):
    """Visualize the pipeline that was created

    Args:
        etl(EtlPipeline): pipeline object that needs to be visualized
        filename(str): filepath for saving the graph
    """
    # Import pygraphviz for plotting the graphs
    try:
        import pygraphviz
    except ImportError:
        raise ImportError('Install pygraphviz for visualizing pipelines')

    if filename is None:
        raise ETLInputError('Filename must be provided for visualization')

    graph = pygraphviz.AGraph(name=etl.name, directed=True, label=etl.name)

    pipeline_objects = etl.pipeline_objects()

    # Add nodes for all activities
    for p_object in pipeline_objects:
        if isinstance(p_object, Activity):
            graph.add_node(p_object.id, shape='diamond', color='turquoise',
                           style='filled')
        if isinstance(p_object, MysqlNode):
            graph.add_node(p_object.id, shape='egg', color='beige',
                           style='filled')
        if isinstance(p_object, RedshiftNode):
            graph.add_node(p_object.id, shape='egg', color='goldenrod',
                           style='filled')
        if isinstance(p_object, S3Node):
            graph.add_node(p_object.id, shape='folder', color='grey',
                           style='filled')

    # Add data dependencies
    for p_object in pipeline_objects:
        if isinstance(p_object, Activity):
            if p_object.input:
                if isinstance(p_object.input, list):
                    for ip in p_object.input:
                        graph.add_edge(ip.id, p_object.id)
                else:
                    graph.add_edge(p_object.input.id, p_object.id)
            if p_object.output:
                graph.add_edge(p_object.id, p_object.output.id)

    # Add depends_on dependencies
    for p_object in pipeline_objects:
        if isinstance(p_object, Activity):
            if isinstance(p_object.depends_on, list):
                dependencies = p_object.depends_on
            elif isinstance(p_object.depends_on, Activity):
                dependencies = [p_object.depends_on]
            else:
                continue

            for dependency in dependencies:
                graph.add_edge(dependency.id, p_object.id, color='blue')

    # Plotting the graph with dot layout
    graph.layout(prog='dot')
    graph.draw(filename)
