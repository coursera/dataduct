"""Helper function for CLI scripts
"""
import argparse
import contextlib
import os

from argparse import ArgumentParser
from argparse import RawTextHelpFormatter


@contextlib.contextmanager
def chdir(dirname=None):
    """Get a context to switch to another working directory
    """
    curdir = os.getcwd()
    try:
        if dirname is not None:
            os.chdir(dirname)
        yield
    finally:
        os.chdir(curdir)


def config_singleton_setup(args):
    """Setup the config singleton based on the mode in args

    Note:
        To instantiate the singleton object with the correct state as this is
        the single entry point to the library. We can use the __new__ function
        to set the debug_level

        We import inside the function as the singleton declaration should be
        done here and at no other entry point. The same pattern is followed
        at all the entry point scripts.
    """
    mode = args.mode if hasattr(args, 'mode') else None

    import logging
    logger = logging.getLogger(__name__)

    from dataduct.config import Config
    from dataduct.config import logger_configuration

    config = Config(mode=mode)

    # Setup up logging for package
    logger_configuration()

    if mode is not None:
        logger.warning('Running in %s mode', config.mode)
    return config


class DataductHelpAction(argparse._HelpAction):
    """HelpAction class used to render a custom help message
    """
    def __call__(self, parser, namespace, values, option_string=None):
        parser.print_help()
        print ''

        # Retrieve subparsers from parser
        subparsers_actions = [
            action for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)]

        for subparsers_action in subparsers_actions:
            # get all subparsers and print help
            for choice, subparser in subparsers_action.choices.items():
                print "Command '{}'".format(choice)
                print subparser.format_usage()
        parser.exit()


def open_sql_shell(database_type, host_alias=None, **kwargs):
    """Opens a sql shell for MySQL or Redshift
    """

    # late import because we need the Singleton config to be loaded in
    # the dataduct main
    from dataduct.data_access import open_shell
    from dataduct.config import Config
    config = Config()
    if database_type == 'redshift':
        open_shell.open_psql_shell()
    else:
        assert config.mysql.get(host_alias), \
            'host_alias "{}" does not exist in config'.format(host_alias)
        open_shell.open_mysql_shell(sql_creds=config.mysql[host_alias])


# Change the width of the output format
formatter_class = lambda prog: RawTextHelpFormatter(prog, max_help_position=50)


# Help parser for parsing subparsers in help
help_parser = ArgumentParser(
    description='Run Dataduct commands',
    add_help=False,
    formatter_class=formatter_class,
)
help_parser.add_argument(
    '-h',
    '--help',
    action=DataductHelpAction,
    help='Help message',
)

# Mode parser shared across all pipeline subparsers
mode_help = 'Mode or config overrides to use for the commands'
mode_parser = ArgumentParser(
    description=mode_help,
    add_help=False,
)
mode_parser.add_argument(
    '-m',
    '--mode',
    default=None,
    help=mode_help
)

# Options parser shared actions all pipeline run options
pipeline_run_options = ArgumentParser(
    description='Specify actions related to running pipelines',
    add_help=False
)
pipeline_run_options.add_argument(
    '-f',
    '--force',
    action='store_true',
    default=False,
    help='Destroy previous versions of this pipeline, if they exist',
)
pipeline_run_options.add_argument(
    '-t',
    '--time_delta',
    default='0h',
    help='Timedelta the pipeline by x time difference',
)
pipeline_run_options.add_argument(
    '-b',
    '--backfill',
    action='store_true',
    default=False,
    help='Indicates that the timedelta supplied is for a backfill',
)
pipeline_run_options.add_argument(
    '--frequency',
    default=None,
    help='Frequency override for the pipeline',
)

# Pipeline definitions parser
pipeline_definition_help = 'Paths of the pipeline definitions'
pipeline_definition_parser = ArgumentParser(
    description=pipeline_definition_help,
    add_help=False,
)
pipeline_definition_parser.add_argument(
    'pipeline_definitions',
    nargs='+',
    help=pipeline_definition_help,
)

# Table definitions parser
table_definition_help = 'Paths of the table definitions'
table_definition_parser = ArgumentParser(
    description=table_definition_help,
    add_help=False,
)
table_definition_parser.add_argument(
    'table_definitions',
    nargs='+',
    help=table_definition_help,
)

# Execute SQL parser
execute_sql_parser_help = 'Execute the query'
execute_sql_parser = ArgumentParser(
    description=execute_sql_parser_help,
    add_help=False,
)
execute_sql_parser.add_argument(
    '-e',
    '--execute',
    action='store_true',
    default=False,
    help='Executes the query',
)


# Single Table definition parser
single_table_definition_help = 'Path of a table definition'
single_table_definition_parser = ArgumentParser(
    description=single_table_definition_help,
    add_help=False,
)
single_table_definition_parser.add_argument(
    'table_definition',
    help=single_table_definition_help,
)


# Filepath input parser
filepath_help = 'Filepath input for storing output of actions'
file_parser = ArgumentParser(
    description=filepath_help,
    add_help=False,
)
file_parser.add_argument(
    dest='filename',
    help='Filename to store output of commands',
)

# S3 Filepath input parser
s3_path_help = 'S3 Path'
s3_path_parser = ArgumentParser(
    description=s3_path_help,
    add_help=False,
)
s3_path_parser.add_argument(
    dest='s3_path',
    help='S3 Path',
)

# database parser
host_alias_help = 'MySQL Host Alias'
host_alias_parser = ArgumentParser(
    description=host_alias_help,
    add_help=False,
)
host_alias_parser.add_argument(
    dest='host_alias',
    help='MySQL Host Alias'
)
