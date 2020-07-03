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

import flowserv.config.base as config


"""Environment variable that contains the database connection string."""
FLOWSERV_DB = 'FLOWSERV_DATABASE'


# -- Public helper method to access configuration values ----------------------

def DB_CONNECT(raise_error=False):
    """Get the database connect string from the environment variable
    'FLOWSERV_DATABASE'. Raises a MissingConfigurationError if the raise_error
    flag is True.

    Parameters
    ----------
    raise_error: bool, default=False
        Flag indicating whether an error is raised if the environment variable
        is not set (i.e., None or and empty string '')

    Returns
    -------
    string

    Raises
    ------
    flowserv.error.MissingConfigurationError
    """
    return config.get_variable(
        name=FLOWSERV_DB,
        raise_error=raise_error
    )
