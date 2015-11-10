#!/usr/bin/env python

"""Script that checks for primary key violations on the input table
"""

from dataduct.steps.utils.primary_key_check import primary_key_check


def main():
    primary_key_check()

if __name__ == '__main__':
    main()
