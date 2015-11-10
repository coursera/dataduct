#!/usr/bin/env python

"""Script that checks if the rows of the destination table has been populated
with the correct values
"""

from dataduct.steps.utils.column_check import column_check

def main():
    column_check()

if __name__ == '__main__':
    main()
