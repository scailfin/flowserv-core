"""This is an adopted version of the REANA-Demo-HelloWorld demo. The main
intend is for testing of the simple multi process backend.
"""

from __future__ import absolute_import, print_function

import argparse
import errno
import os
import sys
import time


def hello(inputfile, outputfile, greeting='Hello', sleeptime=0.0):
    """Write greeting for every name in a given input file to the output file.
    The optional waiting period delays the output between each input name.

    """
    # detect names to greet:
    names = []
    with open(inputfile, 'r') as f:
        for line in f.readlines():
            names.append(line.strip())

    # ensure output directory exists:
    # influenced by http://stackoverflow.com/a/12517490
    if not os.path.exists(os.path.dirname(outputfile)):
        try:
            os.makedirs(os.path.dirname(outputfile))
        except OSError as exc:  # guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    # write greetings:
    with open(outputfile, "at") as f:
        for name in names:
            f.write(greeting + " " + name + "!\n")
            f.flush()
            time.sleep(sleeptime)


if __name__ == '__main__':
    args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--inputfile", required=True)
    parser.add_argument("-o", "--outputfile", required=True)
    parser.add_argument("-g", "--greeting", default='Hello', required=False)
    parser.add_argument("-s", "--sleeptime", default=1.0, type=float, required=False)

    parsed_args = parser.parse_args(args)

    hello(
        inputfile=parsed_args.inputfile,
        outputfile=parsed_args.outputfile,
        greeting=parsed_args.greeting,
        sleeptime=parsed_args.sleeptime
    )
