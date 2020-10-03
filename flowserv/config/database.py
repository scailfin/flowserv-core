# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Defines environment variables and default values that are used to control
the configuration of the underlying database for the benchmark API.

The name of methods that provide access to values from environment variables
are in upper case to emphasize that they access configuration values that are
expected to remain constant throughout the lifespan of a running application.
"""

from typing import Optional

from flowserv.config.base import get_variable


"""Environment variable that contains the database connection string."""
FLOWSERV_DB = 'FLOWSERV_DATABASE'
FLOWSERV_WEBAPP = 'FLOWSERV_WEBAPP'


# -- Public helper method to access configuration values ----------------------

def DB_CONNECT(value: Optional[str] = None) -> str:
    """Get the database connect string from the environment variable
    'FLOWSERV_DATABASE'.  If a connect string was given (e.g., by the user)
    that value will be returned. Otherwise, the value is retrieved from the
    respective environment variable. If the environment variable is not set a
    MissingConfigurationError is raised.

    Parameters
    ----------
    value: string, default=None
        User-provided value.

    Returns
    -------
    string

    Raises
    ------
    flowserv.error.MissingConfigurationError
    """
    if value is not None:
        return value
    return get_variable(
        name=FLOWSERV_DB,
        raise_error=True
    )


def WEBAPP(value: Optional[bool] = None) -> bool:
    """Get the value for the database as web application flag. The web_app flag
    is configured using the FLOWSERV_WEBAPP environment variable. If a default
    value s given that value will be returned. If the variable is not set the
    returned default value is False.

    Parameters
    ----------
    value: bool, default=None
        User-provided value.

    Returns
    -------
    bool
    """
    if value is not None:
        return value
    value = get_variable(
        name=FLOWSERV_WEBAPP,
        default_value=None,
        raise_error=False
    )
    if value is None:
        return False
    if value in ['True', 'true']:
        return True
    else:
        return False
