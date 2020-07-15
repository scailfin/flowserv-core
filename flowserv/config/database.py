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

from flowserv.config.base import get_variable


"""Environment variable that contains the database connection string."""
FLOWSERV_DB = 'FLOWSERV_DATABASE'


# -- Public helper method to access configuration values ----------------------

def DB_CONNECT(value=None):
    """Get the database connect string from the environment variable
    'FLOWSERV_DATABASE'.  If a connect string was given (e.g., by the user)
    that value will be returned. Otherwise, the value is retrieved from the
    respective environment variable. If the environment variable is not set a
    MissingConfigurationError is raised.

    Parameters
    ----------
    value: bool, default=None
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
