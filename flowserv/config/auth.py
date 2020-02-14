# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Defines environment variables and default values that are used to control
the configuration of the authentication modules.

The name of methods that provide access to values from environment variables
are in upper case to emphasize that they access configuration values that are
expected to remain constant throughout the lifespan of a running application.
"""

import flowserv.config.base as config


"""Names of environment variables that are used to configure the authentication
module.
"""
# Time period for which an API key is valid
FLOWSERV_AUTH_LOGINTTL = 'FLOWSERV_AUTH_TTL'

"""Default values for environment variables."""
DEFAULT_LOGINTTL = 24 * 60 * 60

# -- Public helper methods to access configuration values ---------------------


def AUTH_LOGINTTL(default_value=None, raise_error=False):
    """Get the connect string for the database from the respective environment
    variable 'FLOWSERV_AUTH_LOGINTTL'. Raises a MissingConfigurationError if the
    raise_error flag is True and 'FLOWSERV_AUTH_LOGINTTL' is not set. If the
    raise_error flag is False and 'FLOWSERV_AUTH_LOGINTTL' is not set the default
    value is returned.

    Parameters
    ----------
    default_value: string, optional
        Default value if 'FLOWSERV_AUTH_LOGINTTL' is not set and raise_error flag is
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
        name=FLOWSERV_AUTH_LOGINTTL,
        default_value=default_value,
        raise_error=raise_error
    )
    # If the environment variable is not set and no default value was given
    # return the defined default value
    if val is None:
        val = DEFAULT_LOGINTTL
    else:
        try:
            val = int(val)
        except ValueError:
            val = DEFAULT_LOGINTTL
    return val
