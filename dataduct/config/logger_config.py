"""
Script that has the base logger configurations
"""
import os
import logging
from logging.handlers import RotatingFileHandler
from logging import StreamHandler

from .config import Config
from .constants import CONFIG_DIR
from .constants import LOG_FILE

DATE_FMT = '%m-%d %H:%M'
LOG_FMT = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'

config = Config()

def logger_configuration():
    """Set the logger configurations for dataduct
    """
    if not os.path.exists(CONFIG_DIR):
        os.makedir(CONFIG_DIR)

    log_directory = os.path.join(os.path.expanduser(CONFIG_DIR))
    file_name = LOG_FILE
    if hasattr(config, 'logging') and 'LOG_DIR' in config.logging:
        log_directory = config.logging.get('LOG_DIR')
        file_name = config.logging.get('LOG_FILE')

    logging.basicConfig(level=logging.DEBUG,
                        format=LOG_FMT,
                        datefmt=DATE_FMT)

    file_handler = RotatingFileHandler(os.path.join(log_directory, file_name),
                                       maxBytes=200000, backupCount=10)
    console_handler = StreamHandler()
    console_handler.setLevel(logging.WARNING)

    logging.getLogger('').addHandler(file_handler)
    logging.getLogger('').addHandler(console_handler)
