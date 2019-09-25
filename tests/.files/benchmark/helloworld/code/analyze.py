"""Analytics code for the adopted hello workd Demo. Reads a text file (as
produced by the helloworld.py code) and outputs the average number of characters
per line and the number of characters in the line with the most characters.
"""

from __future__ import absolute_import, division, print_function

import argparse
import errno
import os
import json
import sys


def main(inputfile, outputfile):
    """Write greeting for every name in a given input file to the output file.
    The optional waiting period delays the output between each input name.

    """
    # Count number of lines, characters, and keep track of the longest line
    max_line = ''
    total_char_count = 0
    line_count = 0
    with open(inputfile, 'r') as f:
        for line in f:
            line = line.strip()
            line_length = len(line)
            total_char_count += line_length
            line_count += 1
            if line_length > len(max_line):
                max_line = line
    # Create results object
    results = {
        'avg_count': total_char_count / line_count,
        'max_len': len(max_line),
        'max_line': max_line
    }
    # Write analytics results. Ensure that output directory exists:
    # influenced by http://stackoverflow.com/a/12517490
    if not os.path.exists(os.path.dirname(outputfile)):
        try:
            os.makedirs(os.path.dirname(outputfile))
        except OSError as exc:  # guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    with open(outputfile, "at") as f:
        json.dump(results, f)


if __name__ == '__main__':
    args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--inputfile", required=True)
    parser.add_argument("-o", "--outputfile", required=True)

    parsed_args = parser.parse_args(args)

    main(inputfile=parsed_args.inputfile, outputfile=parsed_args.outputfile)
