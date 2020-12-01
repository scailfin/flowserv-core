# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) [2019-2020] NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper methods to get flowserv client configuration parameters from
the environment.
"""

import os

from flowserv.config.api import API_URL


"""Environment variables for the command line interface."""
# Access token for the command line interface
FLOWSERV_ACCESS_TOKEN = 'FLOWSERV_ACCESS_TOKEN'
# Identifier of the default benchmark
ROB_BENCHMARK = 'ROB_BENCHMARK'
# Identifier of the default submission
ROB_SUBMISSION = 'ROB_SUBMISSION'


def ACCESS_TOKEN() -> str:
    """Short cut to get the value of the access token from the environment.
    If the variable is not set an empty string is returned.

    Returns
    -------
    string
    """
    token = os.environ.get(FLOWSERV_ACCESS_TOKEN)
    if token is None:
        return ''
    else:
        return token


def BENCHMARK_ID(default_value: Optional[str] = None) -> str:
    """Short cut to get the value for the default benchmark identifier from the
    environment.

    Returns
    -------
    string
    """
    benchmark_id = os.environ.get(ROB_BENCHMARK)
    if benchmark_id is None:
        return default_value
    else:
        return benchmark_id


def SUBMISSION_ID(default_value: Optional[str] = None) -> str:
    """Short cut to get the value for the default submission identifier from the
    environment.

    Returns
    -------
    string
    """
    submission_id = os.environ.get(ROB_SUBMISSION)
    if submission_id is None:
        return default_value
    else:
        return submission_id
