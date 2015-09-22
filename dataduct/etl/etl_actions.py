"""Script that parses the pipeline definition and has action functions
"""
import yaml

from ..pipeline import Activity
from ..pipeline import MysqlNode
from ..pipeline import RedshiftNode
from ..pipeline import S3Node
from ..utils.exceptions import ETLInputError
from ..utils.helpers import make_pipeline_url
from ..utils.hook import hook
from .etl_pipeline import ETLPipeline

import logging
logger = logging.getLogger(__name__)


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
    extension = file_path.split('.').pop()
    if extension != 'yaml':
        raise ETLInputError('Pipeline definition should have a yaml extention')
    with open(file_path) as f:
        definition = yaml.load(f.read())

        # remove the variables key from the pipeline definition
        # http://stackoverflow.com/questions/4150782/using-yaml-with-variables
        definition.pop('variables', None)
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
    etl.create_teardown_step()
    logger.info('Created pipeline. Name: %s', etl.name)
    return etl


def validate_pipeline(etl, force=False):
    """Validates the pipeline that was created

    Args:
        etl(EtlPipeline): pipeline object that needs to be validated
        force(bool): delete if a pipeline of same name exists
    """
    if force:
        etl.delete_if_exists()
    etl.validate()
    logger.debug(yaml.dump(etl.pipeline.aws_format))
    logger.info('Validated pipeline. Id: %s', etl.pipeline.id)


@hook('activate_pipeline')
def activate_pipeline(etl):
    """Activate the pipeline that was created

    Args:
        etl(EtlPipeline): pipeline object that needs to be activated
    """
    etl.activate()
    logger.info('Activated pipeline. Id: %s', etl.pipeline.id)
    logger.info('Monitor pipeline here: %s',
                make_pipeline_url(etl.pipeline.id))


def visualize_pipeline(etl, activities_only=False, filename=None):
    """Visualize the pipeline that was created

    Args:
        etl(EtlPipeline): pipeline object that needs to be visualized
        filename(str): filepath for saving the graph
    """
    # Import pygraphviz for plotting the graphs
    try:
        import pygraphviz
    except ImportError:
        logger.error('Install pygraphviz for visualizing pipelines')
        raise

    if filename is None:
        raise ETLInputError('Filename must be provided for visualization')

    logger.info('Creating a visualization of %s', etl.name)
    graph = pygraphviz.AGraph(name=etl.name, directed=True, label=etl.name)
    pipeline_objects = etl.pipeline_objects()

    # Add nodes for all activities
    for p_object in pipeline_objects:
        if isinstance(p_object, Activity):
            graph.add_node(p_object.id, shape='rect', color='turquoise',
                           style='filled')
        if not activities_only:
            if isinstance(p_object, MysqlNode):
                graph.add_node(p_object.id, shape='oval', color='beige',
                               style='filled')
            if isinstance(p_object, RedshiftNode):
                graph.add_node(p_object.id, shape='oval', color='goldenrod',
                               style='filled')
            if isinstance(p_object, S3Node):
                graph.add_node(p_object.id, shape='folder', color='grey',
                               style='filled')

    # Add data dependencies
    if not activities_only:
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

        if not activities_only and isinstance(p_object, S3Node):
            for dependency in p_object.dependency_nodes:
                graph.add_edge(dependency.id, p_object.id, color='grey')

    # Plotting the graph with dot layout
    graph.tred()
    graph.layout(prog='dot')
    graph.draw(filename)
