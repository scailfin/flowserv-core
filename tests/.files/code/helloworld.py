"""This is an adopted version of the REANA-Demo-HelloWorld demo. The main
intend is for testing of the simple multi process backend.
"""

import errno
import os
import sys


def hello(inputfile, outputfile, greeting):
    """Write greeting for every name in a given input file to the output file.
    This version of the code writes the name first and then the greeting (in
    lower case).
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
            f.write(f'{name}, {greeting.lower()}!\n')
            f.flush()


if __name__ == '__main__':
    args = sys.argv[1:]

    hello(
        inputfile=args[0],
        outputfile=args[1],
        greeting=args[2]
    )
