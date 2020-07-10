"""Post-processing code for hello workd Demo. This code is included with
flowServ for testing purposes only.
"""

import argparse
import errno
import os
import json
import sys
import time

from flowserv.service.postproc.client import Runs

import flowserv.util as util


def main(rundir, outputfile):
    """Write greeting for every name in a given input file to the output file.
    The optional waiting period delays the output between each input name.

    """
    # Read avg_count for all runs in the ranking
    results = list()
    for run in Runs(rundir):
        filename = run.get_file(name='results/analytics.json')
        doc = util.read_object(filename=filename)
        results.append(doc)
        # Delay execution to allow for testing running post-processing
        # workflows
        time.sleep(1)
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
    parser.add_argument("-r", "--runs", required=True)
    parser.add_argument("-o", "--outputfile", required=True)

    parsed_args = parser.parse_args(args)

    main(rundir=parsed_args.runs, outputfile=parsed_args.outputfile)
