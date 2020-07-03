# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Defines helper methods to access the values of environment variables that
are used for application configuration.
"""

import os

import flowserv.error as err


"""Environment variable for test purposes."""
FLOWSERV_TEST = 'FLOWSERV_TEST'


def get_variable(name, default_value=None, raise_error=None):
    """Get the value for the given  environment variable. Raises a
    MissingConfigurationError if the raise_error flag is True and the variable
    is not set. If the raise_error flag is False and the environment variables
    is not set then the default value is returned.

    Parameters
    ----------
    name: string
        Environment variable name
    default_value: string, optional
        Default value if variable is not set and raise_error flag is False
    raise_error: bool
        Flag indicating whether an error is raised if the environment variable
        is not set (i.e., None or and empty string '')

    Returns
    -------
    string

    Raises
    ------
    flowserv.error.MissingConfigurationError
    """
    value = os.environ.get(name)
    if value is None or value == '':
        if raise_error:
            raise err.MissingConfigurationError(name)
        else:
            value = default_value
    return value
