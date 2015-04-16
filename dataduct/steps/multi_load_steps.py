"""ETL step wrapper for loading into redshift, reload and primary key check
"""
from .create_load_redshift import CreateAndLoadStep
from .reload import ReloadStep
from .primary_key_check import PrimaryKeyCheckStep
from .upsert import UpsertStep


def get_multi_params(step_param, upsert):
    multi_steps = []
    step_param.pop('step_class')
    create_redshift_param = {}
    reload_param = {}
    pk_check_param = {}

    name = ''
    if 'name' in step_param:
        name = step_param.pop('name')
    create_redshift_param = step_param.copy()
    create_redshift_param['step_class'] = CreateAndLoadStep
    if 'destination' in create_redshift_param:
        create_redshift_param.pop('destination')
    multi_steps.append(create_redshift_param)

    reload_param = step_param.copy()
    reload_param['step_class'] = UpsertStep if upsert else ReloadStep
    if 'table_definition' in reload_param:
        reload_param['source'] = reload_param.pop('table_definition')
    multi_steps.append(reload_param)

    pk_check_param = step_param.copy()
    pk_check_param.pop('destination')
    pk_check_param['step_class'] = PrimaryKeyCheckStep
    pk_check_param['depends_on'] = 'UpsertStep0' if upsert else 'ReloadStep0'
    if name != '':
        pk_check_param['name'] = name
    multi_steps.append(pk_check_param)

    return multi_steps


class MultiLoadSteps():
    """StagingLoadProdCheck Step class that load into redshift, reload and pk check
    """
    @staticmethod
    def get_multi_step_params(step_param):
        return get_multi_params(step_param, False)
