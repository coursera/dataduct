#!/usr/bin/env python
"""Simple reducer for the word count example"""

from itertools import groupby
from operator import itemgetter
import sys

def read_mapper_output(file, separator='\t'):
    """Reads the stdin line by line
    """
    for line in file:
        yield line.rstrip().split(separator, 1)

def main(separator='\t'):
    """Read the key value pairs and count the number of words
    Args:
        separator(str): Separator to be used between key and value
    """

    # input comes from STDIN (standard input)
    data = read_mapper_output(sys.stdin, separator=separator)
    # groupby groups multiple word-count pairs by word,
    # and creates an iterator that returns consecutive keys and their group:
    #   current_word - string containing a word (the key)
    for current_word, group in groupby(data, itemgetter(0)):
        try:
            total_count = sum(int(count) for current_word, count in group)
            print "%s%s%d" % (current_word, separator, total_count)
        except ValueError:
            # count was not a number, so silently discard this item
            pass

if __name__ == "__main__":
    main()
