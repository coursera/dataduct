#!/usr/bin/env python
"""Simple mapper for word count example"""

import sys

def read_input(file):
    """Reads the stdin line by line
    """
    for line in file:
        # split the line into words
        yield line.split()

def main(separator='\t'):
    """Read the data and split the lines and emit the words
    Args:
        separator(str): Separator to be used between key and value
    """
    # input comes from STDIN (standard input)
    data = read_input(sys.stdin)
    for words in data:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        for word in words:
            print '%s%s%d' % (word, separator, 1)

if __name__ == "__main__":
    main()
