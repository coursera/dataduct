#!/usr/bin/env python
"""
This script initiates the different calls needed when running
a transform step with the script_directory argument
"""

from dataduct.steps.executors.runner import script_runner

def main():
    script_runner()

if __name__ == '__main__':
    main()
