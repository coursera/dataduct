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
    _root_config = load_yaml(get_config_files())
    _isInstantiated = False
    _root_mode = None

    def __new__(cls, mode=None):
        """Runs once during class instantiation from the cli file
        """
        if not cls._isInstantiated:
            if mode is not None:
                if mode not in cls._root_config:
                    raise ValueError('Specified mode not found in config')

                # Override the select fields specified based on mode
                for key in cls._root_config[mode]:
                    cls._root_config[key].update(cls._root_config[mode][key])

            cls._isInstantiated = True
            cls._root_mode = mode

        obj = super(Config, cls).__new__(cls)
        return obj

    def __init__(self, mode=None):
        """Constructor for the config class
        """
        self.__dict__ = self._root_config

    def __str__(self):
        """String output for the config object
        """
        return yaml.dump(self._root_config, default_flow_style=False)

    def raw_config(self):
        """String formatted config file
        """
        return self.__str__()

    @property
    def mode(self):
        """Mode which the config was created in
        """
        return self._root_mode
