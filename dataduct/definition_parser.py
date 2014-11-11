"""
Script that parses the pipeline definition from the yaml schema
"""
import yaml

from .etl_pipeline import ETLPipeline
from .utils.exceptions import ETLInputError


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
