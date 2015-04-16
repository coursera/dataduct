"""ETL step wrapper for loading into redshift, upsert and primary key check
"""
from .multi_load_steps import get_multi_params


class MultiUpsertSteps():
    """MultiUpsert Step class that creates redshift, upsert and PK check
    """
    @staticmethod
    def get_multi_step_params(step_param):
        return get_multi_params(step_param, True)
