# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""This module defines environment variables and their default values that are
used to configure the API. In addition, the module provides methods to access
the configuration values in the environment variables.
"""

import flowserv.config.base as config


"""Names of environment variables that are used to configure the API."""
# Base directory to store uploaded files and submission results
FLOWSERV_API_BASEDIR = 'FLOWSERV_API_DIR'
# Host name for API server
FLOWSERV_API_HOST = 'FLOWSERV_API_HOST'
# Name of the API instance
FLOWSERV_API_NAME = 'FLOWSERV_API_NAME'
# API application path
FLOWSERV_API_PATH = 'FLOWSERV_API_PATH'
# API server port on host
FLOWSERV_API_PORT = 'FLOWSERV_API_PORT'
# Url protocol prefix
FLOWSERV_API_PROTOCOL = 'FLOWSERV_API_PROTOCOL'


"""Default values for environment variables."""
DEFAULT_DIR = '.flowserv'
DEFAULT_HOST = 'localhost'
DEFAULT_NAME = 'Reproducible and Reusable Data Analysis Workflow Server (API)'
DEFAULT_PATH = '/flowserv/api/v1'
DEFAULT_PORT = 5000
DEFAULT_PROTOCOL = 'http'


# -- Public helper methods to access configuration values ---------------------


def API_BASEDIR(default_value=None, raise_error=False):
    """Get the base directory that is used by the API to store benchmark
    templates and benchmark runs from the respective environment variable
    'FLOWSERV_API_DIR'. Raises a MissingConfigurationError if the raise_error
    flag is True and 'FLOWSERV_API_DIR' is not set. If the raise_error flag is
    False and 'FLOWSERV_API_DIR' is not set the default name is returned.

    Parameters
    ----------
    default_value: string, optional
        Default value if 'FLOWSERV_API_DIR' is not set and raise_error flag is
        False
    raise_error: bool, optional
        Flag indicating whether an error is raised if the environment variable
        is not set (i.e., None or and empty string '')

    Returns
    -------
    string

    Raises
    ------
    flowserv.core.error.MissingConfigurationError
    """
    val = config.get_variable(
        name=FLOWSERV_API_BASEDIR,
        default_value=default_value,
        raise_error=raise_error
    )
    # If the environment variable is not set and no default value was given
    # return the defined default value
    if val is None:
        val = DEFAULT_DIR
    return val


def API_HOST():
    """Get the API server host name from the respective environment variable
    'FLOWSERV_API_HOST'. If the variable is not set the default host name is
    returned.

    Returns
    -------
    string
    """
    return config.get_variable(
        name=FLOWSERV_API_HOST,
        default_value=DEFAULT_HOST,
        raise_error=False
    )


def API_NAME():
    """Get the service name for the API from the respective environment variable
    'FLOWSERV_API_NAME'. If the variable is not set the default name is returned.

    Returns
    -------
    string
    """
    return config.get_variable(
        name=FLOWSERV_API_NAME,
        default_value=DEFAULT_NAME,
        raise_error=False
    )


def API_PATH():
    """Get the application path name for the API from the respective environment
    variable 'FLOWSERV_API_PATH'. If the variable is not set the default application
    path is returned.

    Returns
    -------
    string
    """
    return config.get_variable(
        name=FLOWSERV_API_PATH,
        default_value=DEFAULT_PATH,
        raise_error=False
    )


def API_PORT():
    """Get the API application port number from the respective environment
    variable 'FLOWSERV_API_PORT'. If the variable is not set the default port
    number is returned.

    Expects a value that can be cast to integer. Raises ValueError if the value
    for the environment variable 'FLOWSERV_API_PORT' cannot be cast to integer.

    Returns
    -------
    int

    Raises
    ------
    ValueError
    """
    val = config.get_variable(
        name=FLOWSERV_API_PORT,
        default_value=DEFAULT_PORT,
        raise_error=False
    )
    return int(val)


def API_PROTOCOL():
    """Get the HTTP protocol prefix for urls from the respective environment
    variable 'FLOWSERV_API_PROTOCOL'. If the variable is not set the default
    protocol is returned.

    Returns
    -------
    string

    Raises
    ------
    ValueError
    """
    return config.get_variable(
        name=FLOWSERV_API_PROTOCOL,
        default_value=DEFAULT_PROTOCOL,
        raise_error=False
    )


def API_URL():
    """Get the base URL for the API from the respective environment variables
    'FLOWSERV_API_HOST', 'FLOWSERV_API_PATH', and 'FLOWSERV_API_PORT'.

    Returns
    -------
    string

    Raises
    ------
    ValueError
    """
    protocol = API_PROTOCOL()
    host = API_HOST()
    port = API_PORT()
    if port != 80:
        host = '{}:{}'.format(host, port)
    path = API_PATH()
    if not path.startswith('/'):
        path = '/' + path
    return '{}://{}{}'.format(protocol, host, path)
