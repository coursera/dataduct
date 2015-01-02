#!/usr/bin/env python
"""
This script initiates the different calls needed when running
a transform step with the script_directory argument
"""

# imports
import argparse
import os
import subprocess


def run_command(arguments):
    """
    Args:
        arguments(list of str): Arguments to be executed as a command. Arguments
            are passed as if calling subprocess.call() directly
    """
    return subprocess.call(arguments)


def main():
    """
    Parses the command line arguments and runs the suitable functions
    """
    parser = argparse.ArgumentParser()
    # Environment variable for the source directory
    parser.add_argument('--INPUT_SRC_ENV_VAR', dest='input_src_env_var')

    # Argument for script name
    parser.add_argument('--SCRIPT_NAME', dest='script_name')
    args, ext_script_args = parser.parse_known_args()

    # Check if the source directory exists
    input_src_dir = os.getenv(args.input_src_env_var)
    if not os.path.exists(input_src_dir):
        raise Exception(input_src_dir + " does not exist")

    run_command(['ls', '-l', input_src_dir])
    run_command(['chmod', '-R', '+x', input_src_dir])
    run_command(['ls', '-l', input_src_dir])

    input_file = os.path.join(input_src_dir, args.script_name)
    result = run_command([input_file] + ext_script_args)
    if result != 0:
        raise Exception("Script failed.")


if __name__ == '__main__':
    main()
