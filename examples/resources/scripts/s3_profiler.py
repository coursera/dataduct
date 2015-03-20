#!/usr/bin/env python
"""Walk over files in S3 output node and provide basic information about them
"""

import argparse
from os import getenv
from os import popen
from os import walk
from os import stat
from os.path import join
from os.path import exists


def run_command(command):
    """Execute a shell command

    Args:
        command(list of str): list of command arguments

    Returns:
        output(str): stdout of command
    """
    return popen(' '.join(command)).readlines().pop().strip()


def recurse_directory(directory_path):
    """Recursively walk directories and output basic stats on files

    Args:
        directory_path(str): Path to the directory which is read

    Returns:
        result(list of tuples): (filename, count of lines in file, size of file)
    """
    result = []
    for root, _, files in walk(directory_path):
        for f in files:
            filename = join(root, f)
            result.append((
                filename,
                run_command(['wc', '-l', filename]).split(' ').pop(0),
                str(stat(filename).st_size),
                ))
    return result


def paths_exist(input_directory, paths):
    """Check if a path exists or not

    Args:
        input_directory(str): input directory to be checked
        paths(list of str): paths for which one should check the existence.
            These are paths defined from the input_directory
    """
    for path in paths:
        path = join(input_directory, path)
        if not exists(path):
            raise ValueError('Error: <%s> does not exist' % path)


def profile_input(input_directory, output_directory,
                  output_file_name, fail_if_empty):
    """Lists statistics for all files located in input directrory.
    Output is written to a file in the output directory.

    Args:
        input_directory(path): path to the input directory
        output_directory(path): path to the output directory
        output_file_name(str): filename of the output file
        fail_if_empty(bool): fail script if empty file is found
    """
    files = recurse_directory(input_directory)
    count_of_files = len(files)
    count_of_lines = sum([int(a[1]) for a in files])
    total_size = sum([int(a[2]) for a in files])


    if fail_if_empty and (count_of_files == 0 or count_of_lines == 0):
        assert False, "Error: Input directory is empty."

    lines = ['======================================================']
    lines.append('Profiler Summary')
    lines.append("Count of files:     {0}".format(count_of_files))
    lines.append("Count of lines:     {0}".format(count_of_lines))
    lines.append("Total size (bytes): {0}".format(total_size))

    lines.append('======================================================')
    lines.append('List of Files (filename, count of lines, size of file)')
    lines.extend(['\t'.join(a) for a in files])

    with open(join(output_directory, output_file_name), 'w') as f:
        f.write('\n'.join(lines) + '\n')


def main():
    """Main Function"""
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', required=True)
    parser.add_argument('-o', '--output', default=False)
    parser.add_argument('-n', '--output_file_name', default='out.txt')
    parser.add_argument('-f', '--fail_if_empty', action='store_true',
                        default=False)
    parser.add_argument('paths',
                        nargs='*',
                        help='Instead of profiling, you '+ \
                             'can check whether paths exist.')
    args = parser.parse_args()
    input_dir = getenv(args.input) if getenv(args.input) else args.input

    if len(args.paths) > 0:
        # check whether the paths exist
        paths_exist(input_dir, args.paths)
    else:
        # profile the directory
        assert args.output, "Output directory was not provided."
        output_dir = getenv(args.output) if getenv(args.output) else args.output
        profile_input(input_dir, output_dir, args.output_file_name,
                      args.fail_if_empty)

if __name__ == '__main__':
    main()
