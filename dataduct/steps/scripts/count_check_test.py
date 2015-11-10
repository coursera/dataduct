#!/usr/bin/env python

"""Script that compares the number of rows in the source select script with the
number of rows in the destination table
"""

from dataduct.steps.utils.count_check import count_check

def main():
    count_check()

if __name__ == '__main__':
    main()
