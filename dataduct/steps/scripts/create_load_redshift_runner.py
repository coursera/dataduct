#!/usr/bin/env python

"""Replacement for the load step to use the redshift COPY command instead
"""

import argparse
from ..lib.create_load_redshift import create_load_redshift_runner

def main():
    """Main Function
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--table_definition', dest='table_definition',
                        required=True)
    parser.add_argument('--max_error', dest='max_error', default=0, type=int)
    parser.add_argument('--replace_invalid_char', dest='replace_invalid_char',
                        default=None)
    parser.add_argument('--no_escape', action='store_true', default=False)
    parser.add_argument('--gzip', action='store_true', default=False)
    parser.add_argument('--command_options', dest='command_options', default=None)
    parser.add_argument('--s3_input_paths', dest='input_paths', nargs='+')
    parser.add_argument('--force_drop_table', dest='force_drop_table', default=False)
    args = parser.parse_args()
    print args

    create_load_redshift_runner(**args)

if __name__ == '__main__':
    main()
