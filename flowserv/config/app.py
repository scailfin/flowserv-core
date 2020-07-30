# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""This module defines environment variables and their default values that are
used to configure a flowServ application. In addition, the module provides
methods to get and set the configuration values in the environment variables.
"""

import os

from flowserv.config.api import API_BASEDIR as APP_BASEDIR  # noqa: F401
from flowserv.config.base import get_variable
from flowserv.config.controller import FLOWSERV_ASYNC


"""Names of environment variables that configure the application."""
# Identifier of the workflow group that was created when installing the
# application.
FLOWSERV_APP = 'FLOWSERV_APP'


def APP_KEY(value=None):
    """Get the application key from the respective environment variable
    'FLOWSERV_APP'. If a user-provided key is given, that value is returned.
    Otherwise, if the environment variable is not set an error is raised.

    Parameters
    ----------
    value: string, default=None
        User-provided application identifier.

    Returns
    -------
    string
    """
    if value is not None:
        return value
    return get_variable(name=FLOWSERV_APP, raise_error=True)


def SYNC():
    """Set the asyncronous engine flag in the respective environment variable
    to False.
    """
    os.environ[FLOWSERV_ASYNC] = 'False'
