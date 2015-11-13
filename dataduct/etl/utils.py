"""Utility functions for processing etl steps
"""
import imp
from ..config import Config
from ..steps import *  # noqa
from ..utils.helpers import parse_path
from ..utils.exceptions import ETLInputError

STEP_CLASSES = {
    'column-check': ColumnCheckStep,
    'count-check': CountCheckStep,
    'create-load-redshift': CreateAndLoadStep,
    'create-update-sql': CreateUpdateSqlStep,
    'emr-step': EMRJobStep,
    'emr-streaming': EMRStreamingStep,
    'extract-local': ExtractLocalStep,
    'extract-rds': ExtractRdsStep,
    'extract-redshift': ExtractRedshiftStep,
    'extract-s3': ExtractS3Step,
    'load-redshift': LoadRedshiftStep,
    'load-reload-pk': LoadReloadAndPrimaryKeyStep,
    'pipeline-dependencies': PipelineDependenciesStep,
    'primary-key-check': PrimaryKeyCheckStep,
    'qa-transform': QATransformStep,
    'reload': ReloadStep,
    'sql-command': SqlCommandStep,
    'transform': TransformStep,
    'upsert': UpsertStep,
}


def get_custom_steps():
    """Fetch the custom steps specified in config
    """
    config = Config()
    custom_steps = dict()

    for step_def in getattr(config, 'custom_steps', list()):
        step_type = step_def['step_type']
        path = parse_path(step_def['file_path'], 'CUSTOM_STEPS_PATH')

        # Load source from the file path provided
        step_mod = imp.load_source(step_type, path)

        # Get the step class based on class_name provided
        step_class = getattr(step_mod, step_def['class_name'])

        # Check if step_class is of type ETLStep
        if not issubclass(step_class, ETLStep):
            raise ETLInputError('Step type %s is not of type ETLStep' %
                                step_class.__name__)

        custom_steps[step_type] = step_class
    return custom_steps


STEP_CONFIG = STEP_CLASSES.copy()
STEP_CONFIG.update(get_custom_steps())


def process_steps(steps_params):
    """Format the step parameters by changing step type to step class
    """
    steps = []
    for step_param in steps_params:
        params = step_param.copy()
        step_type = params.pop('step_type')
        params['step_class'] = STEP_CONFIG[step_type]
        steps.append(params)
    return steps
