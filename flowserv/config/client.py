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

from typing import Optional

import os


"""Environment variables for the command line interface."""
# Access token for the command line interface
FLOWSERV_ACCESS_TOKEN = 'FLOWSERV_ACCESS_TOKEN'
# Define the type of client that the command line interface uses.
FLOWSERV_CLIENT = 'FLOWSERV_CLIENT'
# Identifier of the default benchmark
ROB_BENCHMARK = 'ROB_BENCHMARK'
# Identifier of the default submission
ROB_SUBMISSION = 'ROB_SUBMISSION'


def ACCESS_TOKEN() -> str:
    """Shortcut to get the value of the access token from the environment.
    If the variable is not set an empty string is returned.

    Returns
    -------
    string
    """
    return os.environ.get(FLOWSERV_ACCESS_TOKEN)


def BENCHMARK_ID(default: Optional[str] = None) -> str:
    """Shortcut to get the value for the default benchmark identifier from the
    environment.

    Parameters
    ----------
    default: str
        Default value that is returned if the environment variable is not set.

    Returns
    -------
    string
    """
    benchmark_id = os.environ.get(ROB_BENCHMARK)
    if benchmark_id is None:
        return default
    else:
        return benchmark_id


def CLIENT() -> str:
    """Shortcut to get the client type identifier from the environment.
    If the variable is not set, 'LOCAL' is returned as the default.

    Returns
    -------
    string
    """
    return os.environ.get(FLOWSERV_CLIENT, 'LOCAL')


def SUBMISSION_ID(default: Optional[str] = None) -> str:
    """Shortcut to get the value for the default submission identifier from the
    environment.

    Parameters
    ----------
    default: str
        Default value that is returned if the environment variable is not set.

    Returns
    -------
    string
    """
    submission_id = os.environ.get(ROB_SUBMISSION)
    if submission_id is None:
        return default
    else:
        return submission_id
