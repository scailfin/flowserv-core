# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""This module defines environment variables that are used to configure the
backend that controls workflow execution. Different workflow controller may
define additional environment variables for their configuration.
"""

import os

# Name of the class that implements the workflow controller interface
FLOWSERV_BACKEND_CLASS = 'FLOWSERV_BACKEND_CLASS'
# Name of the module that contains the workflow controller implementation
FLOWSERV_BACKEND_MODULE = 'FLOWSERV_BACKEND_MODULE'


def CLEAR_BACKEND():
    """Clear environment variables for the backend."""
    if FLOWSERV_BACKEND_MODULE in os.environ:
        del os.environ[FLOWSERV_BACKEND_MODULE]
    if FLOWSERV_BACKEND_CLASS in os.environ:
        del os.environ[FLOWSERV_BACKEND_CLASS]


def DEFAULT_BACKEND():
    """Set environment variables for the default backend."""
    os.environ[FLOWSERV_BACKEND_MODULE] = 'flowserv.controller.serial.engine'
    os.environ[FLOWSERV_BACKEND_CLASS] = 'SerialWorkflowEngine'
