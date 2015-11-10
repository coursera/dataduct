#!/usr/bin/env python

"""Replacement for the load step to use the redshift COPY command instead
"""

from dataduct.steps.utils.create_load_redshift import create_load_redshift_runner

def main():
    create_load_redshift_runner()

if __name__ == '__main__':
    main()
