# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Defines environment variables and default values that are used to control
the configuration of the authentication modules.

The name of methods that provide access to values from environment variables
are in upper case to emphasize that they access configuration values that are
expected to remain constant throughout the lifespan of a running application.
"""

import os

from typing import Optional

from flowserv.config.base import get_variable


"""Names of environment variables that are used to configure the authentication
module.
"""
# Time period for which an API key is valid
FLOWSERV_AUTH_LOGINTTL = 'FLOWSERV_AUTH_TTL'
# Authentication policy
FLOWSERV_AUTH = 'FLOWSERV_AUTH'


"""Default values for environment variables."""
DEFAULT_LOGINTTL = 24 * 60 * 60
# Access policies
DEFAULT_AUTH = 'DEFAULT'
OPEN_ACCESS = 'OPEN'


# -- Public helper methods to access configuration values ---------------------


def AUTH_LOGINTTL(value: Optional[str] = None) -> str:
    """Get the connect string for the database from the respective environment
    variable 'FLOWSERV_AUTH_LOGINTTL'. If a user-provided value is given it
    will be returned. If the environment variable is not set the default value
    will be returned.

    Parameters
    ----------
    value: int, default=None
        User-provided value for the property.

    Returns
    -------
    int
    """
    if value is not None:
        return value
    # Ensure that the value can be converted to int. If not, the default value
    # is returned.
    val = get_variable(
        name=FLOWSERV_AUTH_LOGINTTL,
        default_value=DEFAULT_LOGINTTL,
        raise_error=False
    )
    try:
        return int(val)
    except ValueError:
        return DEFAULT_LOGINTTL


def AUTH_POLICY() -> str:
    """Get the authentication policy variable value from the environment
    variable 'FLOWSERV_AUTH'. The default authentication policy is 'DEFAULT.'

    Returns
    -------
    string
    """
    return os.environ.get(FLOWSERV_AUTH, DEFAULT_AUTH)
