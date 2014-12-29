"""Module that maintains the config singleton object used across the package
"""
import os
import yaml


def get_config_files():
    """Get the config file for dataduct

    Note:
        The order of precedence is:
        1. /etc/dataduct.cfg
        2. ~/.dataduct
        3. DATADUCT_PATH environment variable
    """
    dataduct_config_path = '/etc/dataduct.cfg'
    dataduct_user_config_path = os.path.join(os.path.expanduser('~/.dataduct'))
    config_files = [dataduct_config_path, dataduct_user_config_path]

    # Check DATADUCT_PATH env variable for other configuration locations
    if 'DATADUCT_PATH' in os.environ:
        for path in os.environ['DATADUCT_PATH'].split(":"):
            config_files.append(os.path.expanduser(path))

    return config_files


def load_yaml(configFiles):
    """Load the config files based on environment settings
    """
    for configFile in configFiles:
        try:
            return yaml.load(open(configFile, 'r'))
        except (OSError, IOError):
            continue
    raise Exception('Dataduct config file is missing')


class Config(object):
    """Config singleton to manage changes config variables across the package
    """
    _shared_config = load_yaml(get_config_files())

    def __init__(self):
        """Constructor for the config class
        """
        self.__dict__ = self._shared_config

    def __str__(self):
        """String output for the config object
        """
        return yaml.dump(self._shared_config, default_flow_style=False)

    def raw_config(self):
        """String formatted config file
        """
        return self.__str__()
