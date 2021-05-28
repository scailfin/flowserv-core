# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods for worker classes unit tests."""

import flowserv.util as util


def a_plus_b(a: int, b: int) -> int:
    """Simple helper function for testing code steps.

    Returns the sum of the two arguments.
    """
    return a + b


def multi_by_x(filename: str, x: int) -> int:
    """Read input file with single integer value (in Json format) and multiplies
    the value with the given x.

    Expects a Json object with format: {"value": v}

    Returns the multiplication result.
    """
    doc = util.read_object(filename=filename)
    return doc['value'] * x
