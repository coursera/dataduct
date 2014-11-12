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
    for configFile in configFiles:
        try:
            return yaml.load(open(configFile, 'r'))
        except (OSError, IOError):
            continue


class Config(object):
    _shared_config = load_yaml(DataductConfigFiles)

    def __init__(self):
        self.__dict__ = self._shared_config
