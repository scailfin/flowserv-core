# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Helper method to get current values for all flowServ configuration
parameters.
"""

import os

from typing import Dict

from flowserv.config.api import (
    API_BASEDIR, API_HOST, API_NAME, API_PATH, API_PORT, API_PROTOCOL,
    FLOWSERV_API_BASEDIR, FLOWSERV_API_HOST, FLOWSERV_API_NAME,
    FLOWSERV_API_PATH, FLOWSERV_API_PORT, FLOWSERV_API_PROTOCOL
)
from flowserv.config.auth import FLOWSERV_AUTH_LOGINTTL, AUTH_LOGINTTL
from flowserv.config.backend import (
    FLOWSERV_BACKEND_CLASS, FLOWSERV_BACKEND_MODULE
)
from flowserv.config.database import FLOWSERV_DB, DB_CONNECT

import flowserv.error as err


def get_configuration() -> Dict:
    """Get values for configuration parameters. The result is a dictionary of
    dictionaries. At the first level, each dictionary groups configuration
    parameter for a logical component of the system. These groups are keyed by
    the component title. Each component dictionary maps an environment variable
    to its current value.

    The keys for the component groups are:

    - Web Service API
    - Authentication
    - Database
    - File Store
    - Workflow Controller

    Returns
    -------
    dict
    """
    configuration = dict()
    # Configuration for the API
    apiconf = dict()
    apiconf[FLOWSERV_API_BASEDIR] = API_BASEDIR()
    apiconf[FLOWSERV_API_NAME] = '"{}"'.format(API_NAME())
    apiconf[FLOWSERV_API_HOST] = API_HOST()
    apiconf[FLOWSERV_API_PORT] = API_PORT()
    apiconf[FLOWSERV_API_PROTOCOL] = API_PROTOCOL()
    apiconf[FLOWSERV_API_PATH] = API_PATH()
    configuration['Web Service API'] = apiconf
    # Configuration for user authentication
    authconf = dict()
    authconf[FLOWSERV_AUTH_LOGINTTL] = AUTH_LOGINTTL()
    configuration['Authentication'] = authconf
    # Configuration for the underlying database
    dbconf = dict()
    try:
        connect_url = DB_CONNECT()
    except err.MissingConfigurationError:
        connect_url = 'None'
    dbconf[FLOWSERV_DB] = connect_url
    configuration['Database'] = dbconf
    # Configuration for the file store
    fsconf = dict()
    from flowserv.service.files import get_filestore
    for key, val in get_filestore(raise_error=False).configuration():
        fsconf[key] = val
    configuration['File Store'] = fsconf
    # Configuration for the workflow execution backend
    backendconf = dict()
    from flowserv.service.backend import init_backend
    backend_class = os.environ.get(FLOWSERV_BACKEND_CLASS, '')
    backendconf[FLOWSERV_BACKEND_CLASS] = backend_class
    backend_module = os.environ.get(FLOWSERV_BACKEND_MODULE, '')
    backendconf[FLOWSERV_BACKEND_MODULE] = backend_module
    for key, val in init_backend(raise_error=False).configuration():
        backendconf[key] = val
    configuration['Workflow Controller'] = backendconf
    return configuration
