import os
import yaml

# We look at (in order of precedence):
# /etc/dataduct.cfg and ~/.dataduct for configuration constants

DataductConfigPath = '/etc/.dataduct'
DataductUserConfigPath = os.path.join(os.path.expanduser('~/.dataduct'))
DataductConfigFiles = [DataductConfigPath, DataductUserConfigPath]

# Check DATADUCT_PATH env variable for other configuration locations
if 'DATADUCT_PATH' in os.environ:
    for path in os.environ['DATADUCT_PATH'].split(":"):
        DataductConfigFiles.append(os.path.expanduser(path))


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
    _shared_config = load_yaml(DataductConfigFiles)

    def __init__(self):
        self.__dict__ = self._shared_config
