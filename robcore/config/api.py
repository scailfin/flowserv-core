# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Defines environment variables and their default values that are used to
control the configurtion of the API. Also provides methods to access the
configuration values.
"""

import robcore.config.base as config


"""Names of environment variables that are used to configure the authentication
module.
"""
# Base URL for all API resources
ROB_API_URL = 'ROB_APIURL'
# Base directory to store uploaded files and submission results
ROB_API_BASEDIR = 'ROB_APIDIR'
# Name of the API instance
ROB_API_NAME = 'ROB_APINAME'


"""Default values for environment variables."""
DEFAULT_DIR = '.rob'
DEFAULT_NAME = 'Reproducible Open Benchmarks for Data Analysis (API)'
DEFAULT_URL = 'http://localhost:5000/rob/api/v1'

# -- Public helper methods to access configuration values ----------------------

def API_BASEDIR(default_value=None, raise_error=False):
    """Get the base directory that is used by the API to store benchmark
    templates and benchmark runs from the respective environment variable
    'ROB_APIDIR'. Raises a MissingConfigurationError if thr raise_error flag
    is True and 'ROB_APIDIR' is not set. If the raise_error flag is False and
    'ROB_APIDIR' is not set the default name is returned.

    Parameters
    ----------
    default_value: string, optional
        Default value if 'ROB_APIDIR' is not set and raise_error flag is
        False
    raise_error: bool, optional
        Flag indicating whether an error is raised if the environment variable
        is not set (i.e., None or and empty string '')

    Returns
    -------
    string

    Raises
    ------
    robcore.error.MissingConfigurationError
    """
    val = config.get_variable(
        name=ROB_API_BASEDIR,
        default_value=default_value,
        raise_error=raise_error
    )
    # If the environment variable is not set and no default value was given
    # return the defined default value
    if val is None:
        val = DEFAULT_DIR
    return val


def API_NAME(default_value=None, raise_error=False):
    """Get the service name for the API from the respective environment variable
    'ROB_APINAME'. Raises a MissingConfigurationError if thr raise_error flag
    is True and 'ROB_APINAME' is not set. If the raise_error flag is False and
    'ROB_APINAME' is not set the default name is returned.

    Parameters
    ----------
    default_value: string, optional
        Default value if 'ROB_APINAME' is not set and raise_error flag is
        False
    raise_error: bool, optional
        Flag indicating whether an error is raised if the environment variable
        is not set (i.e., None or and empty string '')

    Returns
    -------
    string

    Raises
    ------
    robcore.error.MissingConfigurationError
    """
    val = config.get_variable(
        name=ROB_API_NAME,
        default_value=default_value,
        raise_error=raise_error
    )
    # If the environment variable is not set and no default value was given
    # return the defined default value
    if val is None:
        val = DEFAULT_NAME
    return val


def API_URL(default_value=None, raise_error=False):
    """Get the base URL for the API from the respective environment variable
    'ROB_API_URL'. Raises a MissingConfigurationError if thr raise_error flag
    is True and 'ROB_API_URL' is not set. If the raise_error flag is False and
    'ROB_API_URL' is not set the default value is returned.

    Parameters
    ----------
    default_value: string, optional
        Default value if 'ROB_API_URL' is not set and raise_error flag is
        False
    raise_error: bool, optional
        Flag indicating whether an error is raised if the environment variable
        is not set (i.e., None or and empty string '')

    Returns
    -------
    string

    Raises
    ------
    robcore.error.MissingConfigurationError
    """
    val = config.get_variable(
        name=ROB_API_URL,
        default_value=default_value,
        raise_error=raise_error
    )
    # If the environment variable is not set and no default value was given
    # return the defined default value
    if val is None:
        val = DEFAULT_URL
    return val
