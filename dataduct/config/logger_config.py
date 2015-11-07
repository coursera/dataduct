"""Script that has the base logger configurations
"""
import os
import logging
from logging.handlers import RotatingFileHandler

from .config import Config
from .constants import CONFIG_DIR
from .constants import LOG_FILE

FILE_FORMAT_STR = '%(asctime)s [%(levelname)s]: %(message)s ' + \
                  '[in %(name)s:%(lineno)d in %(funcName)s]'
CONSOLE_FORMAT_STR = '[%(levelname)s]: %(message)s'


def logger_configuration():
    """Set the logger configurations for dataduct
    """
    config = Config()

    if not hasattr(config, 'logging'):
        raise Exception('logging section is missing in config')

    log_directory = os.path.expanduser(config.logging.get(
        'LOG_DIR', os.path.join('~', CONFIG_DIR)))
    file_name = config.logging.get(
        'LOG_FILE', LOG_FILE)

    console_level = config.logging.get(
        'CONSOLE_DEBUG_LEVEL', logging.INFO)
    file_level = config.logging.get(
        'FILE_DEBUG_LEVEL', logging.DEBUG)

    if not os.path.exists(log_directory):
        os.mkdir(log_directory)

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    file_handler = RotatingFileHandler(os.path.join(log_directory, file_name),
                                       maxBytes=200000,
                                       backupCount=10)
    file_handler.setLevel(file_level)
    file_handler.setFormatter(logging.Formatter(FILE_FORMAT_STR,
                                                datefmt='%Y-%m-%d %H:%M'))

    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(logging.Formatter(CONSOLE_FORMAT_STR))

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
