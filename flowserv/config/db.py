# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Defines environment variables and default values that are used to control
the configuration of the underlying database for the benchmark API.

The name of methods that provide access to values from environment variables
are in upper case to emphasize that they access configuration values that are
expected to remain constant throughout the lifespan of a running application.
"""

import flowserv.config.base as config


"""Environment variables to configure the database driver."""
ROB_DB_ID = 'ROB_DBMS'


# -- Public helper method to access configuration values ----------------------

def DB_IDENTIFIER(default_value=None, raise_error=False):
    """Get the identifier for the database management system from the respective
    environment variable 'ROB_DB_ID'. Raises a MissingConfigurationError if
    the raise_error flag is True and 'ROB_DB_ID' is not set. If the
    raise_error flag is False and 'ROB_DB_ID' is not set the default value is
    returned.

    Parameters
    ----------
    default_value: string, optional
        Default value if 'ROB_DB_ID' is not set and raise_error flag is False
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
    return config.get_variable(
        name=ROB_DB_ID,
        default_value=default_value,
        raise_error=raise_error
    )
