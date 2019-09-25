# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Export the JSON schema for template parameter declarations. Prints the
schema either in JSON or YAML format to standard outout or an optional output
file.
"""

import json
import sys
import yaml

from robcore.model.template.parameter.declaration import PARAMETER_SCHEMA


def main(args):
    """Print or write JSON schema for parameter declaration. Expects at most two
    arguments:

    1) Output format (either JSON or YAML)
    2) Output file

    If only one argument is given it is assumed to be the output format. If no
    output file is given the schema will be printed to STDOUT. If no arguments
    are given the default output format is JSON.

    Parameters
    ----------
    args: list(string)
        List of command line arguments.
    """
    # Ensure that at most two arguments are given
    if len(args) > 2:
        print('Usage: {[JSON | YAML]} {<output-file>}')
        sys.exit(-1)
    # If arguments are given the first argument is assumed to be the format
    # specifiaction (either JSON or YAML). The optional second argument
    # specifies the output file.
    output_file = None
    if len(args) > 0:
        format = args[0].upper()
        if len(args) == 2:
            output_file = args[1]
    else:
        format = 'JSON'
    # Ensure that the format specification is valid.
    if format == 'JSON':
        if not output_file is None:
            with open(output_file, 'w') as f:
                json.dump(PARAMETER_SCHEMA, f)
        else:
            print(json.dumps(PARAMETER_SCHEMA, indent=4))
    elif format == 'YAML':
        if not output_file is None:
            with open(output_file, 'w') as f:
                yaml.dump(PARAMETER_SCHEMA, f)
        else:
            print(yaml.dump(PARAMETER_SCHEMA))
    else:
        print('Invalid format specification \'{}\''.format(args[0]))
        print('Usage: {[JSON | YAML]} {<output-file>}')
        sys.exit(-1)


if __name__ == '__main__':
    main(sys.argv[1:])
